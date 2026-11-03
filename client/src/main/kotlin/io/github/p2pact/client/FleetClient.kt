package io.github.p2pact.client

import java.util.concurrent.CompletableFuture

// Tool 2 `p2pact.fleet` (fleet spec): the computer and its agent sessions. JSON-RPC over `2.1 rpc`.

/** The computer as the fleet describes it (`Node`). */
class FleetNode(
    val id: String,
    val name: String,
    val os: String?,
    /** online, lid, battery, temperature, working… as the computer reports. */
    val status: JsonMap,
    val agents: List<AgentInfo>,
    val capabilities: JsonMap,
    val raw: JsonMap,
) {
    companion object {
        fun parse(m: JsonMap): FleetNode {
            val caps = m.obj("capabilities") ?: emptyMap()
            return FleetNode(m.str("id") ?: "", m.str("name") ?: "", m.str("os"), m.obj("status") ?: emptyMap(),
                caps.objs("agents").map { AgentInfo.parse(it) }, caps, m)
        }
    }
}

class FleetHello(val protocol: String?, val node: FleetNode, val raw: JsonMap)

/** An agent session of the fleet (`Session`): the same id as the chat's. */
class FleetSession(
    val id: String,
    val agent: String?,
    val model: String?,
    val dir: String?,
    val keep: Boolean?,
    val perms: String?,
    /** starting, working, waiting, idle… as the computer says. */
    val phase: String?,
    val title: String?,
    /** RFC 3339. */
    val updated: String?,
    /** The `ask` of the status, if the agent waits for the person. */
    val ask: Any?,
    val raw: JsonMap,
) {
    companion object {
        fun parse(m: JsonMap): FleetSession {
            val spec = m.obj("spec") ?: emptyMap()
            val st = m.obj("status") ?: emptyMap()
            return FleetSession(m.str("id") ?: "", spec.str("agent"), spec.str("model"), spec.str("dir"), spec.bool("keep"),
                spec.str("perms"), st.str("phase"), st.str("title"), st.str("updated"), st["ask"], m)
        }
    }
}

/** What `session/create` starts. [extra] is added as it is, for fields this SDK does not know. */
class SessionSpec(
    val agent: String? = null,
    val dir: String? = null,
    val model: String? = null,
    val keep: Boolean? = null,
    val perms: String? = null,
    val extra: JsonMap = emptyMap(),
) {
    fun toMap(): JsonMap {
        val m = LinkedHashMap<String, Any?>()
        if (agent != null) m["agent"] = agent
        if (dir != null) m["dir"] = dir
        if (model != null) m["model"] = model
        if (keep != null) m["keep"] = keep
        if (perms != null) m["perms"] = perms
        m.putAll(extra)
        return m
    }
}

class FleetMessage(val seq: Long, val role: String, val text: String, val time: String?, val raw: JsonMap) {
    companion object {
        fun parse(m: JsonMap) = FleetMessage(m.long("seq") ?: 0, m.str("role") ?: "", m.str("text") ?: "", m.str("time"), m)
    }
}

/** `session/messages`: the replies after a number; [older] — there are earlier ones. */
class FleetMessages(val messages: List<FleetMessage>, val last: Long?, val older: Boolean, val raw: JsonMap)

class CloseResult(val ok: Boolean, /** Done, but not smoothly. */ val warning: String?, val raw: JsonMap)

/** An event of a watch (fleet spec §4). */
sealed class FleetEvent {
    /** The whole list, at every change. */
    class SessionsChanged(val sessions: List<FleetSession>) : FleetEvent()

    /** New replies of a session. */
    class SessionMessages(val id: String, val messages: List<FleetMessage>) : FleetEvent()

    /** Replies of [id] were lost: read `messages(id, after)` again. */
    class Resync(val id: String) : FleetEvent()

    /** The agent's text while it is written (only if the watch asked for `partial`). */
    class Partial(val id: String, val message: JsonMap) : FleetEvent()

    /** What the computer would show as a notification. */
    class Notify(val text: String) : FleetEvent()

    class Other(val method: String, val params: JsonMap) : FleetEvent()
}

/** An active watch: close it to unsubscribe. */
class FleetWatch internal constructor(
    /** The sessions at the start. */
    val sessions: List<FleetSession>,
    private val onClose: () -> Unit,
) : AutoCloseable {
    override fun close() = onClose()
}

class FleetClient internal constructor(private val computer: Computer) {
    /** The events of the watch ([watch]); also after the computer restarted (the watch is repeated). */
    val events = Events<FleetEvent>()

    @Volatile private var watching: Boolean? = null // partial, or null — not watching

    private fun call(method: String, params: JsonMap? = null, timeoutMs: Long = computer.owner.config.callTimeoutMs): CompletableFuture<JsonMap> =
        computer.rpc(Tools.FLEET_NUMBER, method, params, streamOf(params?.str("id")), timeoutMs)

    // --- read ---

    fun hello(): CompletableFuture<FleetHello> = call("fleet/hello").map {
        FleetHello(it.str("protocol"), FleetNode.parse(it.obj("node") ?: emptyMap()), it)
    }

    fun node(): CompletableFuture<FleetNode> = call("node/get").map { FleetNode.parse(it) }

    fun sessions(): CompletableFuture<List<FleetSession>> = call("session/list").map { r -> r.objs("sessions").map { FleetSession.parse(it) } }

    /** session is `session/get`; no such session — [RpcException] `-32004`. */
    fun session(id: String): CompletableFuture<FleetSession> = call("session/get", mapOf("id" to id)).map { FleetSession.parse(it) }

    /** messages is `session/messages`: the replies after number [after] (without it, the last [limit]). */
    fun messages(id: String, after: Long? = null, limit: Int? = null): CompletableFuture<FleetMessages> {
        val p = LinkedHashMap<String, Any?>()
        p["id"] = id
        if (after != null) p["after"] = after
        if (limit != null) p["limit"] = limit
        return call("session/messages", p).map { r ->
            FleetMessages(r.objs("messages").map { FleetMessage.parse(it) }, r.long("last"), r.bool("older") ?: false, r)
        }
    }

    /**
     * watch subscribes to the fleet: [events] get the changes. Close the result to stop. With
     * [partial] the agent's text comes while it is written.
     */
    fun watch(partial: Boolean = false): CompletableFuture<FleetWatch> {
        watching = partial
        return call("fleet/watch", mapOf("partial" to partial)).map { r ->
            FleetWatch(r.objs("sessions").map { FleetSession.parse(it) }) {
                watching = null
                call("fleet/unwatch")
            }
        }
    }

    // --- write ---

    fun create(spec: SessionSpec, text: String? = null): CompletableFuture<FleetSession> {
        val p = LinkedHashMap<String, Any?>()
        p["spec"] = spec.toMap()
        if (text != null) p["text"] = text
        return call("session/create", p, 0).map { FleetSession.parse(it) }
    }

    /** send puts a message into the session. */
    fun send(id: String, text: String): CompletableFuture<Unit> = call("session/send", mapOf("id" to id, "text" to text), 0).map { }

    /** answer answers an agent's question ([answers]: question → label) or permission ([allow]). */
    fun answer(id: String, waitId: String? = null, answers: Map<String, String>? = null, allow: Boolean? = null): CompletableFuture<Unit> {
        val p = LinkedHashMap<String, Any?>()
        p["id"] = id
        if (waitId != null) p["waitId"] = waitId
        if (answers != null) p["answers"] = answers
        if (allow != null) p["allow"] = allow
        return call("session/answer", p, 0).map { }
    }

    fun interrupt(id: String): CompletableFuture<Unit> = call("session/interrupt", mapOf("id" to id), 0).map { }

    fun close(id: String): CompletableFuture<CloseResult> =
        call("session/close", mapOf("id" to id), 0).map { CloseResult(it.bool("ok") ?: true, it.str("warning"), it) }

    /** setKeep keeps the agent running (on) or lets it stop. */
    fun setKeep(id: String, on: Boolean): CompletableFuture<Unit> = call("session/setKeep", mapOf("id" to id, "on" to on), 0).map { }

    /** The engine's state: version, mode, link, clients, peers (write: it names the device's data directory). */
    fun status(): CompletableFuture<JsonMap> = call("engine/status")

    fun settings(): CompletableFuture<JsonMap> = call("settings/get")

    /** setSettings changes settings and gives them as they are after; invalid [patch] — [RpcException] `-32602`. */
    fun setSettings(patch: JsonMap): CompletableFuture<JsonMap> = call("settings/set", mapOf("patch" to patch))

    /** Any other method of the fleet. */
    fun request(method: String, params: JsonMap? = null): CompletableFuture<JsonMap> = call(method, params)

    // --- events ---

    internal fun reconnected() {
        // the device forgot the subscription with its restart (fleet spec §4)
        val partial = watching ?: return
        call("fleet/watch", mapOf("partial" to partial))
    }

    internal fun notified(args: List<Any?>) {
        val msg = normalize(args.firstOrNull()).asMap() ?: return
        val method = msg.str("method") ?: return
        val p = msg.obj("params") ?: emptyMap()
        events.emit(when (method) {
            "session/changed" -> FleetEvent.SessionsChanged(p.objs("sessions").map { FleetSession.parse(it) })
            "session/messages" -> FleetEvent.SessionMessages(p.str("id") ?: "", p.objs("messages").map { FleetMessage.parse(it) })
            "session/resync" -> FleetEvent.Resync(p.str("id") ?: "")
            "session/partial" -> FleetEvent.Partial(p.str("id") ?: "", p.obj("message") ?: emptyMap())
            "notify" -> FleetEvent.Notify(p.str("text") ?: "")
            else -> FleetEvent.Other(method, p)
        })
    }
}
