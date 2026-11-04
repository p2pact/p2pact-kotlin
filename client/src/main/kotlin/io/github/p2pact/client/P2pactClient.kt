package io.github.p2pact.client

import io.github.p2pact.ALL_TOOLS
import io.github.p2pact.Crypto
import io.github.p2pact.Errors
import io.github.p2pact.Grant
import io.github.p2pact.Invite
import io.github.p2pact.Kind
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import io.github.p2pact.Node
import io.github.p2pact.P2pactError
import io.github.p2pact.Peer
import io.github.p2pact.RelayHint
import io.github.p2pact.grantLevel
import io.github.p2pact.later
import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/** The names of the public tools (core spec §8). */
object Tools {
    const val CHAT = "p2pact.chat"
    const val FLEET = "p2pact.fleet"
    const val PLUGINS = "p2pact.plugins"

    /**
     * The terminal (tool 5, spec term.md): the SDK has no typed client for it, call it with
     * [Computer.callTool]; its events arrive on [P2pactClient.toolEvents] named `p2pact.term`. A
     * computer that serves it under a former name still answers once the alias is declared:
     * `client.node.aliasTool(former, Tools.TERM)`.
     */
    const val TERM = "p2pact.term"

    internal const val CHAT_NUMBER = 1
    internal const val FLEET_NUMBER = 2
    internal const val PLUGINS_NUMBER = 4
}

/**
 * What the SDK itself uses, to ask for when pairing: chat and fleet in full, the plugins' view.
 * The person of the computer may narrow it ([Computer.grants] tells what was granted).
 */
val DEFAULT_WANTS: List<Grant> = listOf(
    Grant(Tools.CHAT, LEVEL_WRITE),
    Grant(Tools.FLEET, LEVEL_WRITE),
    Grant(Tools.PLUGINS, LEVEL_READ),
)

/**
 * ClientConfig is how a client reaches computers. [udp] and [tcp] are the addresses it listens on
 * (`""` for tcp — only dials; null — no such road); [relay] is a relay to use when a computer's
 * invite names none.
 */
data class ClientConfig(
    val udp: String? = "0.0.0.0:0",
    val tcp: String? = "",
    val relay: RelayHint? = null,
    /** How long a call waits for its answer; 0 — for ever. Prompts and questions never time out. */
    val callTimeoutMs: Long = 60_000,
    val pairTimeoutMs: Long = 30_000,
)

enum class ConnectionState {
    /** No live road to the computer. */
    OFFLINE,

    /** A session with the computer runs and it has told its tools. */
    ONLINE,
}

/** A change of a computer the application may show. */
sealed class ComputerEvent {
    abstract val computer: Computer

    /** Pairing finished (on our side). */
    class Paired(override val computer: Computer) : ComputerEvent()

    /** The computer's hello came: a session runs, its tools are known. After a restart it comes again. */
    class Online(override val computer: Computer) : ComputerEvent()

    /** The computer told what we may use of its tools (event 0.6). */
    class GrantsChanged(override val computer: Computer, val grants: List<Grant>) : ComputerEvent()

    class Forgotten(override val computer: Computer) : ComputerEvent()
}

/** An event of a tool the SDK has no typed layer for (a plugin's tool): the numbers are the computer's. */
class ToolEvent(val computer: Computer, val tool: String?, val toolNumber: Int, val event: Int, val args: List<Any?>)

/**
 * P2pactClient is an application's side of P2PACT: it pairs with computers (devices running the
 * P2PACT engine) by their invite, keeps them, and gives the typed clients of their public tools.
 * The state (keys, paired computers) lives in [dir]; keeping that safe is the application's job.
 *
 *     val client = P2pactClient(File(dataDir), "My app")
 *     val computer = client.pair("p2pact1.…").await()
 *     val chats = computer.chat.listSessions().await()
 *
 * Calls return [CompletableFuture]s that fail with [P2pactException] (the P2PACT error codes) or
 * [RpcException] (the tool's own errors); [await] waits for one.
 */
class P2pactClient private constructor(
    /** The protocol node: for what this SDK does not wrap. Do not set its callbacks (unless you wrapped it: see [hello]). */
    val node: Node,
    val config: ClientConfig,
    /** The SDK made the node and runs it; otherwise the application does (see the wrapping constructor). */
    private val owned: Boolean,
) : AutoCloseable {
    constructor(dir: File, name: String, config: ClientConfig = ClientConfig()) : this(Node(dir, name), config, true)

    /**
     * Wrapping constructor: the typed clients over a node the application already runs (it owns
     * the keys, the roads, the pairing and the node's callbacks). The SDK only serves the chat
     * tool's calls from the computers (`handle`, so wrap before `node.start()`); [config]'s roads
     * are not used. The application keeps `onHello`, `onEvent`, `onGranted` and `onPaired` of the
     * node and passes them on: [hello], [event], [granted], [paired]. [close] leaves the node open.
     */
    constructor(node: Node, config: ClientConfig = ClientConfig()) : this(node, config, false)
    private val computers = ConcurrentHashMap<String, Computer>()
    private val sequence = ConcurrentHashMap<String, AtomicLong>()

    /** Pairing, hellos and grants of computers. */
    val computerEvents = Events<ComputerEvent>()

    /** Events of tools without a typed layer (a plugin's tool). */
    val toolEvents = Events<ToolEvent>()

    init {
        if (owned) {
            node.kind = Kind.CLIENT
            config.udp?.let { node.listenUdp(it) }
            config.tcp?.let { node.listenTcp(it) }
            config.relay?.let { node.setRelay(it) }
            node.onPaired = { p -> paired(p) }
            node.onHello = { addr -> hello(addr) }
            node.onGranted = { addr, g -> granted(addr, g) }
            node.onEvent = { addr, tool, ev, args -> event(addr, tool, ev, args) }
        }
        // the computer's calls into the chat tool: permissions and questions of agents (chat spec §6, §9.1)
        node.handle(Tools.CHAT_NUMBER, 1) { addr, args ->
            val c = computerOrNull(addr) ?: throw P2pactError(Errors.FORBIDDEN, "unknown computer")
            later(c.chat.incoming(args))
        }
        if (owned) node.start()
    }

    /** The computer's hello came (the node's `onHello`): for a wrapped node, pass it on first thing. */
    fun hello(addr: String) {
        computerOrNull(addr)?.helloArrived()
    }

    /** An event of a tool from a computer (the node's `onEvent`): for a wrapped node, in the order the node gave it. */
    fun event(addr: String, tool: Int, ev: Int, args: List<Any?>) = dispatch(addr, tool, ev, args)

    /** The computer told what we may use (the node's `onGranted`). */
    fun granted(addr: String, grants: List<Grant>) {
        computerOrNull(addr)?.let { computerEvents.emit(ComputerEvent.GrantsChanged(it, grants)) }
    }

    /** Pairing finished (the node's `onPaired`). */
    fun paired(p: Peer) {
        if (usable(p)) computerEvents.emit(ComputerEvent.Paired(computerOf(p.addr)))
    }

    private fun usable(p: Peer) = p.kind != Kind.PLUGIN && p.kind != Kind.CLIENT

    /**
     * pair pairs with a computer by its invite (`p2pact1.…`, from `nomad invite` or the like) as a
     * client application, asking for [wants]. The future completes when the pairing is done; the
     * person at the computer may narrow the grants later still: [ComputerEvent.GrantsChanged].
     */
    fun pair(invite: String, wants: List<Grant> = DEFAULT_WANTS, timeoutMs: Long = config.pairTimeoutMs): CompletableFuture<Computer> {
        val inv = try { Invite.parse(invite) } catch (e: IllegalArgumentException) { return failed(e) }
        return node.joinAsync(inv, timeoutMs, wants).map { computerOf(it.addr) }
    }

    /** computers lists the paired computers. */
    fun computers(): List<Computer> = node.trust.list().filter { usable(it) }.map { computerOf(it.addr) }

    /** computer finds a paired computer by its address ([Computer.id]) or null. */
    fun computer(id: String): Computer? = computerOrNull(id)

    /** forget unpairs a computer: its keys and conversation state go. */
    fun forget(computer: Computer) {
        node.forget(computer.id)
        computers.remove(computer.id)
        computerEvents.emit(ComputerEvent.Forgotten(computer))
    }

    /** networkChanged tells the client the device's network changed (Wi-Fi to mobile): it listens anew and the sessions stay. */
    fun networkChanged() {
        config.udp?.let { runCatching { node.rebindUdp(it) } }
    }

    override fun close() {
        if (owned) node.close()
    }

    // --- internals ---

    internal fun computerOrNull(addr: String): Computer? =
        computers[addr] ?: node.trust.get(addr)?.takeIf { usable(it) }?.let { computerOf(addr) }

    private fun computerOf(addr: String): Computer = computers.getOrPut(addr) { Computer(this, addr) }

    private fun dispatch(addr: String, tool: Int, ev: Int, args: List<Any?>) {
        val c = computerOrNull(addr) ?: return
        when {
            tool == Tools.CHAT_NUMBER && ev == 1 -> c.chat.notified(args)
            tool == Tools.FLEET_NUMBER && ev == 1 -> c.fleet.notified(args)
            tool == Tools.PLUGINS_NUMBER && ev == 1 -> c.plugins.notified(args)
            tool == Tools.PLUGINS_NUMBER -> {}
            else -> toolEvents.emit(ToolEvent(c, node.peerToolName(addr, tool), tool, ev, args.map { normalize(it) }))
        }
    }

    /** The number of our next notification on a stream: from 1 for each pair of devices and stream (fleet spec §1). */
    internal fun nextEventNumber(addr: String, stream: Long): Long =
        sequence.getOrPut("$addr/$stream") { AtomicLong() }.incrementAndGet()

    internal fun resetEventNumbers(addr: String) {
        sequence.keys.removeAll { it.startsWith("$addr/") }
    }
}

/** The stream of a session or resource: the first 8 bytes of BLAKE2s(id) as a number, 0 replaced by 1 (chat spec §2); no id — 0. */
fun streamOf(id: String?): Long {
    if (id.isNullOrEmpty()) return 0
    val n = ByteBuffer.wrap(Crypto.blake2s(id.toByteArray(Charsets.UTF_8)), 0, 8).long
    return if (n == 0L) 1 else n
}

/**
 * Computer is a paired computer: the device that runs agents. The typed clients of its tools hang
 * off it: [chat], [fleet], [plugins].
 */
class Computer internal constructor(internal val owner: P2pactClient, val id: String) {
    private val node get() = owner.node
    private val peer: Peer? get() = node.trust.get(id)
    private val ready = CompletableFuture<Computer>()
    @Volatile private var hello = false

    val name: String get() = peer?.name ?: ""
    val os: String? get() = peer?.os

    /** Whether a session with the computer runs and it has told its tools. */
    val state: ConnectionState get() = if (hello && node.paths(id).isNotEmpty()) ConnectionState.ONLINE else ConnectionState.OFFLINE

    /** The roads in use: `udp`, `tcp`, `relay:<address>`… */
    val paths: List<String> get() = node.paths(id)

    /**
     * What the computer lets us use of its tools, as it told us (event 0.6). Paired with wants (the SDK always
     * asks): nothing until the first 0.6 arrives. A device that joined without wants has everything until told.
     */
    val grants: List<Grant> get() = node.grantedBy(id) ?: listOf(Grant(ALL_TOOLS, LEVEL_WRITE))

    /** The level we have on a tool of the computer: 0 none, 1 [LEVEL_READ], 2 [LEVEL_WRITE]. Grants on a former name of the tool count (`Node.aliasTool`). */
    fun level(tool: String): Int = node.levelIn(grants, tool)

    fun canRead(tool: String) = level(tool) >= LEVEL_READ
    fun canWrite(tool: String) = level(tool) >= LEVEL_WRITE

    val chat: ChatClient by lazy { ChatClient(this) }
    val fleet: FleetClient by lazy { FleetClient(this) }
    val plugins: PluginsClient by lazy { PluginsClient(this) }

    /** whenOnline completes when the computer's hello has come (at once, if it has). */
    fun whenOnline(): CompletableFuture<Computer> = ready

    internal fun helloArrived() {
        hello = true
        owner.resetEventNumbers(id)
        chat.reconnected()
        fleet.reconnected()
        plugins.reconnected()
        ready.complete(this)
        owner.computerEvents.emit(ComputerEvent.Online(this))
    }

    /** rpc is a JSON-RPC call over the tool's `rpc` method (chat spec §2): the result object, or the tool's error. */
    internal fun rpc(tool: Int, method: String, params: JsonMap?, stream: Long = 0, timeoutMs: Long = owner.config.callTimeoutMs): CompletableFuture<JsonMap> {
        val req = LinkedHashMap<String, Any?>()
        req["jsonrpc"] = "2.0"
        req["method"] = method
        if (params != null) req["params"] = params
        return node.callOn(id, stream, tool, 1, wire(req), timeoutMs = timeoutMs).map { r ->
            val msg = normalize(r.firstOrNull()).asMap() ?: throw FailedException("empty response")
            val err = msg.obj("error")
            if (err != null) throw RpcException(err.int("code") ?: RpcException.FAILED, err.str("message") ?: "", err["data"])
            msg.obj("result") ?: emptyMap()
        }
    }

    /** notify sends a JSON-RPC notification as the tool's `notify` event. */
    internal fun notify(tool: Int, method: String, params: JsonMap, stream: Long) {
        val msg = mapOf("jsonrpc" to "2.0", "method" to method, "params" to params)
        node.eventOn(id, stream, tool, 1, wire(msg), owner.nextEventNumber(id, stream))
    }

    /**
     * toolNumber is the number the computer gave a tool of its own (a plugin's) in its hello; null
     * until it is known.
     */
    fun toolNumber(name: String): Int? = node.toolNumber(id, name)

    /**
     * callTool calls a method of a tool by name; the number is the computer's own (hosted tools,
     * core spec §8.2). It waits for the computer's hello if it has not come. Arguments are JSON
     * trees (numbers, text, lists, maps, bytes); the result values are the same, bytes as they are.
     * A tool of the spec the computer does not announce goes by a former name of it the computer
     * does announce (`Node.aliasTool`): [Tools.TERM] reaches an older computer that named its terminal itself.
     */
    fun callTool(name: String, method: Int, vararg args: Any?, timeoutMs: Long = owner.config.callTimeoutMs): CompletableFuture<List<Any?>> {
        fun go(): CompletableFuture<List<Any?>> {
            val n = node.resolveTool(id, name)?.number ?: return failed(NoMethodException("the computer has no tool $name"))
            return node.callOn(id, 0, n, method, *args, timeoutMs = timeoutMs).map { it }
        }
        if (node.peerHasTool(id, name) || hello) return go()
        // the hello may still be on its way after pairing
        val limit = if (timeoutMs > 0) timeoutMs else 10_000
        return settled(ready, limit).flatMap { go() }
    }
}
