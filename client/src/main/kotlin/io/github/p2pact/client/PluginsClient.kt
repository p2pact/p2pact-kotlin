package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import java.util.concurrent.CompletableFuture

// Tool 4 `p2pact.plugins` (plugins spec): the plugins a computer hosts. JSON-RPC over `4.1 rpc`.
// Install and remove stay on the computer.

enum class PluginStatus(val wire: String) {
    RUNNING("running"),
    STARTING("starting"),
    RESTARTING("restarting"),

    /** Switched off. */
    STOPPED("stopped"),

    /** Disabled after repeated exits. */
    FAILED("failed"),

    /** Not for this system. */
    UNSUPPORTED("unsupported"),

    /** Unreadable manifest. */
    BROKEN("broken"),
    UNKNOWN("");

    companion object {
        fun of(s: String?) = entries.firstOrNull { it.wire == s && s != "" } ?: UNKNOWN
    }
}

class PluginInfo(
    val name: String,
    val version: String?,
    val title: String?,
    val enabled: Boolean,
    val status: PluginStatus,
    val error: String?,
    val restarts: Int,
    val pid: Int?,
    /** The tool names the manifest allows. */
    val tools: List<String>,
    /** The tools the plugin has announced now, a subset of [tools]. */
    val announced: List<String>,
    /** `tool[:read|:write]` the plugin asked for. */
    val needs: List<String>,
    val raw: JsonMap,
) {
    companion object {
        fun parse(m: JsonMap): PluginInfo {
            fun strings(k: String) = m[k].asList()?.mapNotNull { it as? String } ?: emptyList()
            return PluginInfo(m.str("name") ?: "", m.str("version"), m.str("title"), m.bool("enabled") ?: false,
                PluginStatus.of(m.str("status")), m.str("error"), m.int("restarts") ?: 0, m.int("pid"),
                strings("tools"), strings("announced"), strings("needs"), m)
        }
    }
}

/** What a plugin may use of the computer, and which methods of its own tools need only read. */
class PluginGrants(val name: String, val grants: List<Grant>, val read: Map<String, List<Int>>, val raw: JsonMap)

/** An event of the plugins watch (plugins spec §2). */
sealed class PluginEvent {
    /** [plugin] as `plugins/list` gives it, now; [removed] means it is gone (the last entry). */
    class Changed(val plugin: PluginInfo, val removed: Boolean) : PluginEvent()

    class Other(val method: String, val params: JsonMap) : PluginEvent()
}

/** An active plugins watch: close it to unsubscribe. */
class PluginsWatch internal constructor(
    /** The plugins at the start. */
    val plugins: List<PluginInfo>,
    private val onClose: () -> Unit,
) : AutoCloseable {
    override fun close() = onClose()
}

/** The engine sends the level as "read"/"write"; an integer (1, 2) is accepted too. */
internal fun levelOf(v: Any?): Int? = when (v) {
    is Number -> v.toInt()
    "read" -> LEVEL_READ
    "write" -> LEVEL_WRITE
    else -> null
}

class PluginsClient internal constructor(private val computer: Computer) {
    /** The events of the watch ([watch]); also after the computer restarted (the watch is repeated). */
    val events = Events<PluginEvent>()

    @Volatile private var watching = false

    /** watch is `plugins/watch`: [events] get `plugins/changed`. Close the result to stop. */
    fun watch(): CompletableFuture<PluginsWatch> {
        watching = true
        return call("plugins/watch").map { r ->
            PluginsWatch(r.objs("plugins").map { PluginInfo.parse(it) }) {
                unwatch()
            }
        }
    }

    /** unwatch is `plugins/unwatch`. */
    fun unwatch(): CompletableFuture<Unit> {
        watching = false
        return call("plugins/unwatch").map { }
    }

    internal fun reconnected() {
        // the device forgot the subscription with its restart (plugins spec §2)
        if (watching) call("plugins/watch")
    }

    internal fun notified(args: List<Any?>) {
        val msg = normalize(args.firstOrNull()).asMap() ?: return
        val method = msg.str("method") ?: return
        val p = msg.obj("params") ?: emptyMap()
        events.emit(when (method) {
            "plugins/changed" -> PluginEvent.Changed(PluginInfo.parse(p), p.bool("removed") ?: false)
            else -> PluginEvent.Other(method, p)
        })
    }

    private fun call(method: String, params: JsonMap? = null) = computer.rpc(Tools.PLUGINS_NUMBER, method, params)

    /** list is `plugins/list`. */
    fun list(): CompletableFuture<List<PluginInfo>> = call("plugins/list").map { r -> r.objs("plugins").map { PluginInfo.parse(it) } }

    /** log is `plugins/log`: the last [lines] lines of the plugin's log. */
    fun log(name: String, lines: Int? = null): CompletableFuture<String> {
        val p = LinkedHashMap<String, Any?>()
        p["name"] = name
        if (lines != null) p["lines"] = lines
        return call("plugins/log", p).map { it.str("log") ?: "" }
    }

    /** grants is `plugins/grants`. */
    fun grants(name: String): CompletableFuture<PluginGrants> = call("plugins/grants", mapOf("name" to name)).map { r ->
        val grants = r["grants"].asList()?.mapNotNull { g ->
            val m = g.asMap() ?: return@mapNotNull null
            Grant(m.str("tool") ?: return@mapNotNull null, levelOf(m["level"]) ?: return@mapNotNull null)
        } ?: emptyList()
        val read = r.obj("read")?.mapValues { (_, v) -> v.asList()?.mapNotNull { (it as? Number)?.toInt() } ?: emptyList() } ?: emptyMap()
        PluginGrants(r.str("name") ?: name, grants, read, r)
    }

    /** enable starts a plugin (and keeps it on across the computer's restarts). Needs write. */
    fun enable(name: String): CompletableFuture<Unit> = call("plugins/enable", mapOf("name" to name)).map { }

    fun disable(name: String): CompletableFuture<Unit> = call("plugins/disable", mapOf("name" to name)).map { }

    /**
     * callTool calls a method of a tool a plugin of the computer serves (core spec §8.2) by the
     * tool's name; the number is the computer's own. See [Computer.callTool].
     */
    fun callTool(name: String, method: Int, vararg args: Any?): CompletableFuture<List<Any?>> =
        computer.callTool(name, method, *args.map { wire(it) }.toTypedArray())
}
