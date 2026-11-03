package io.github.p2pact.client

import io.github.p2pact.Errors
import io.github.p2pact.Grant
import io.github.p2pact.Json
import io.github.p2pact.Kind
import io.github.p2pact.LEVEL_WRITE
import io.github.p2pact.Node
import io.github.p2pact.P2pactError
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** The engine's reference messages (contract/tools of the engine repository, copied to the test resources). */
object Fixtures {
    fun exists(dir: String, name: String) = Fixtures::class.java.getResource("/contract/tools/$dir/$name.json") != null

    fun read(dir: String, name: String): JsonMap {
        val s = Fixtures::class.java.getResourceAsStream("/contract/tools/$dir/$name.json")!!.readBytes().toString(Charsets.UTF_8)
        return Json.parse(s).asMap()!!
    }

    /** The `params` of a request fixture ({} when it has none). */
    fun params(dir: String, name: String): JsonMap = read(dir, name).obj("params") ?: emptyMap()
}

class Seen(val tool: Int, val method: String, val params: JsonMap, val level: Int)

/**
 * FakeEngine is a computer built from the library alone: it serves tools 1, 2 and 4 and a hosted
 * tool, and answers with the messages of the contract fixtures. What a client sent is in [seen].
 */
class FakeEngine(dir: File) : AutoCloseable {
    val node = Node(dir, "MacBook Pro")
    val seen = CopyOnWriteArrayList<Seen>()

    /** Notifications the client sent (`session/cancel`…), as JSON-RPC messages. */
    val fromClient = LinkedBlockingQueue<JsonMap>()

    /** What the person grants a newcomer that asked for [wants]; by default what it asked. */
    @Volatile var decide: (List<Grant>) -> List<Grant> = { it }

    /** The address of the paired client. */
    val paired = CompletableFuture<String>()

    /** Answers for a method ("1:session/list"), in place of the fixture's. */
    val overrides = ConcurrentHashMap<String, (JsonMap) -> JsonMap>()
    val watchers = LinkedBlockingQueue<Boolean>()
    private val numbers = ConcurrentHashMap<Long, AtomicLong>()

    init {
        node.kind = Kind.COMPUTER
        node.listenUdp("127.0.0.1:0")
        node.onWants = { _, wants -> decide(wants) }
        node.onPaired = { paired.complete(it.addr) }
        node.onEvent = { _, tool, ev, args ->
            if (tool == 1 && ev == 1) normalize(args.firstOrNull()).asMap()?.let { fromClient.add(it) }
        }
        node.handleLevel(1, 1) { _, level, args -> rpc(1, level, args, CHAT_READ) }
        node.handleLevel(2, 1) { _, level, args -> rpc(2, level, args, FLEET_READ) }
        node.handleLevel(4, 1) { _, level, args -> rpc(4, level, args, PLUGINS_READ) }
        node.handleTool("com.example.hello", 1) { _, args -> listOf("hello " + args[0]) }
        node.start()
    }

    fun invite(): String = node.invite(60_000).toString()

    val clientAddr: String get() = paired.get(10, TimeUnit.SECONDS)

    private fun rpc(tool: Int, level: Int, args: List<Any?>, reads: Set<String>): List<Any?> {
        val req = normalize(args[0]).asMap() ?: throw P2pactError(Errors.BAD_ARGS, "no request")
        val method = req.str("method") ?: throw P2pactError(Errors.BAD_ARGS, "no method")
        val params = req.obj("params") ?: emptyMap()
        seen.add(Seen(tool, method, params, level))
        // one P2PACT method carries all of the tool: the level is checked on the JSON-RPC method (chat spec §6)
        if (method !in reads && level < LEVEL_WRITE) throw P2pactError(Errors.FORBIDDEN, "forbidden")
        val resp = overrides["$tool:$method"]?.invoke(params) ?: when (tool) {
            1 -> chat(method, params)
            2 -> fleet(method, params)
            else -> plugins(method, params)
        }
        return listOf(wire(resp))
    }

    private fun chat(method: String, params: JsonMap): JsonMap {
        val stem = method.replace('/', '.')
        if (method == "session/load") {
            for (n in listOf("session.update.user_message", "session.update.agent_message", "session.update.tool_call")) {
                notifyChat(Fixtures.read("p2pact.chat", "$n.notification"))
            }
            return mapOf("jsonrpc" to "2.0", "result" to mapOf("_meta" to mapOf("p2pact" to mapOf("older" to true))))
        }
        return if (Fixtures.exists("p2pact.chat", "$stem.response")) Fixtures.read("p2pact.chat", "$stem.response")
        else mapOf("jsonrpc" to "2.0", "result" to emptyMap<String, Any?>())
    }

    private fun fleet(method: String, params: JsonMap): JsonMap {
        val stem = when (method) {
            "fleet/hello" -> "hello"
            "session/list" -> "session-list"
            "session/get" -> "session-get"
            "session/messages" -> "session-messages"
            "fleet/watch" -> { watchers.add(params.bool("partial") ?: false); "watch" }
            "fleet/unwatch" -> "unwatch"
            "session/create" -> "session-create"
            "session/send" -> if (params.str("id") == "нет") "session-send-unknown" else "session-send"
            "settings/set" -> "settings-set"
            else -> null
        }
        if (method == "node/get") {
            return mapOf("jsonrpc" to "2.0", "result" to Fixtures.read("p2pact.fleet", "hello.response").obj("result")!!.obj("node")!!)
        }
        return if (stem != null) Fixtures.read("p2pact.fleet", "$stem.response")
        else mapOf("jsonrpc" to "2.0", "result" to mapOf("ok" to true))
    }

    private fun plugins(method: String, params: JsonMap): JsonMap {
        val stem = method.replace('/', '.')
        if (method == "plugins/log" && params.str("name") == "com.example.nope") return Fixtures.read("p2pact.plugins", "plugins.log.unknown.response")
        return if (Fixtures.exists("p2pact.plugins", "$stem.response")) Fixtures.read("p2pact.plugins", "$stem.response")
        else mapOf("jsonrpc" to "2.0", "error" to mapOf("code" to -32601, "message" to "no method $method"))
    }

    // --- to the client ---

    /** notify sends a JSON-RPC notification as the tool's event, on the stream of its session or id. */
    fun notify(tool: Int, msg: JsonMap) {
        val p = msg.obj("params") ?: emptyMap()
        val stream = streamOf(p.str("sessionId") ?: p.str("id"))
        node.eventOn(clientAddr, stream, tool, 1, wire(msg), numbers.getOrPut(stream) { AtomicLong() }.incrementAndGet())
    }

    fun notifyChat(msg: JsonMap) = notify(1, msg)

    /** call makes a request of the client, as the agent device does (permissions, questions). */
    fun ask(tool: Int, msg: JsonMap, sessionId: String): CompletableFuture<JsonMap> =
        node.callOn(clientAddr, streamOf(sessionId), tool, 1, wire(msg), timeoutMs = 10_000).thenApply { normalize(it.first()).asMap()!! }

    override fun close() = node.close()

    companion object {
        val CHAT_READ = setOf("initialize", "session/list", "session/load", "_p2pact/older", "_p2pact/commands", "_p2pact/files",
            "_p2pact/history", "_p2pact/projects", "_p2pact/agents", "_p2pact/image", "_p2pact/diff")
        val FLEET_READ = setOf("fleet/hello", "node/get", "session/list", "session/get", "session/messages", "fleet/watch", "fleet/unwatch")
        val PLUGINS_READ = setOf("plugins/list", "plugins/log", "plugins/grants", "plugins/watch", "plugins/unwatch")
    }
}
