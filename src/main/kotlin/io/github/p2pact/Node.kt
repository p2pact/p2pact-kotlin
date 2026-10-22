package io.github.p2pact

import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.FutureTask
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

private const val TS_SKEW = 120 * SEC // an older handshake start is a replay: drop it
private const val INIT_EVERY = SEC // handshake retry while unanswered
private const val AUTO_CONNECT = 30 * SEC // how often to try known devices without a session
private const val ALIVE_FOR = 60 * SEC // a road is alive if something arrived on it within this time
private const val SESSION_IDLE = 180 * SEC

/** keepaliveGap: how long a live road may stay quiet before a keepalive goes on it; tests shorten it. */
@Volatile internal var keepaliveGap = 25 * SEC

/** probeEvery: how often the direct roads are tried while a session runs over a relay or an application road (spec §7); tests shorten it. */
@Volatile internal var probeEvery = 30 * SEC

/** Error codes (spec §6). */
object Errors {
    const val NO_METHOD = 1
    const val BAD_ARGS = 2
    const val NOT_FOUND = 3
    const val FORBIDDEN = 4
    const val FAILED = 5
    const val NOT_DELIVERED = 6
}

/** P2pactError is a call error (spec §6): a code and a text. */
class P2pactError(val code: Int, text: String) : Exception(text)

/**
 * Handler is a tool method: decoded CBOR arguments → result values (or a thrown P2pactError). A
 * handler that would wait long returns [later] instead, so its stream goes on (spec §6).
 */
fun interface Handler {
    fun handle(peer: String, args: List<Any?>): List<Any?>
}

/** Later is a reply taken out of the stream's turn: see [later]. It holds no result values. */
class Later internal constructor(val reply: CompletionStage<List<Any?>>) : AbstractList<Any?>() {
    override val size get() = 0
    override fun get(index: Int): Any? = throw IndexOutOfBoundsException("a reply given later")
}

/**
 * later answers a call later (spec §6): a handler returns later(reply), its turn on the stream
 * ends there, so the stream's next calls and events run (a cancel among them), and the call's
 * result or error goes when reply completes — unless the caller restarted meanwhile.
 *
 *     node.handle(1, 1) { peer, args -> later(CompletableFuture.supplyAsync { runTurn(args) }) }
 */
fun later(reply: CompletionStage<List<Any?>>): List<Any?> = Later(reply)

/** DictState tells what compresses messages to a device (see [Node.dictState]). */
data class DictState(val own: ByteArray?, val using: ByteArray?, val training: Boolean)

private class Session(
    val peer: String,
    val local: Long,
    val remote: Long,
    val send: CipherState,
    val recv: CipherState,
    var lastRecv: Long,
) {
    var sendN = 0L
    val win = Window()
}

private class PendingHs(
    val pair: Boolean,
    val hs: Handshake,
    val at: Long,
    val peer: String = "", // known device: to whom
    val invite: Invite? = null, // pairing: their invite (we join) or ours (they join)
    val done: CompletableFuture<Peer>? = null,
    val remoteIdx: Long = 0, // pairing, we respond: the joiner's index
) {
    // data that overtook the handshake reply (reordering): run once the session is up
    val early = ArrayList<Pair<ByteArray, Endpoint>>()
}

private class EpState(val ep: Endpoint) {
    var lastRecv = 0L
    var lastSend = 0L
    var session = 0L // our index of the session its last packet belonged to
}

private class Inbound(val msg: ByteArray, val zip: Boolean, val stream: Long)

private class PeerState(val addr: String) {
    val st = Delivery()
    val eps = HashMap<String, EpState>()
    var lastTS = 0L // timestamp of the last accepted handshake start
    var boot = 0L // the other device's boot id (from the handshake)
    var heard = false // it knows our boot id: sent us session data or answered our handshake
    var lastInit = 0L
    var lastProbe = 0L // the direct roads were last tried while the session ran over a relay or an application road
    val calls = HashMap<Long, CompletableFuture<List<Any?>>>()
    var callID = 0L
    var dicts: List<ByteArray> = emptyList() // its dictionaries (ids), by slot — from its hello and 0.5
    var lacks = ArrayList<ByteArray>() // it asked us for these, so it lacks them until it announces again
    val waiting = ArrayDeque<Inbound>() // waiting for a dictionary (spec §5): run in order once it arrives
    var waitSince = 0L // when the head of waiting started to wait (unhold)
    var askingInfo = false // our 0.2 info call is on its way (askInfo)
    var askedInfo = 0L
    var fetching = false
    var tried = ArrayList<ByteArray>() // dictionaries of its list we asked it for and did not get
    val sysCalls = HashSet<Long>() // our calls of tool 0: their results are not conversation
    val inbox = HashMap<Long, Inbox>() // calls and events waiting for their turn, per stream

    /** initGap: retry a handshake after three measured round trips (250 ms – 2 s), else 1 s. */
    fun initGap(): Long = if (st.srtt == 0L) INIT_EVERY else (3 * st.srtt).coerceIn(250 * MS, 2 * SEC)
}

/** Inbox runs a stream's calls and events one at a time, in the order they came (spec §6). */
private class Inbox {
    val q = ArrayDeque<() -> Unit>()
    var running = false
}

private fun daemon(name: String) = ThreadFactory { r -> Thread(r, name).apply { isDaemon = true } }

/** goos is the operating system as p2pact-go names it (android and ios are phones). */
internal fun goos(): String {
    if (System.getProperty("java.vm.name")?.contains("Dalvik") == true ||
        System.getProperty("java.vendor")?.contains("Android") == true
    ) return "android"
    val os = (System.getProperty("os.name") ?: "").lowercase()
    return when {
        os.startsWith("mac") || os.contains("darwin") -> "darwin"
        os.startsWith("windows") -> "windows"
        os.contains("linux") -> "linux"
        os.contains("freebsd") -> "freebsd"
        else -> os.ifEmpty { "unknown" }
    }
}

/**
 * Node is a device in a P2PACT network (spec §1–§8): pairing, sessions over any road, delivery,
 * messages and compression. All protocol state lives on one thread (the node's loop); roads
 * receive on their own threads and hand packets to it. Calls and events run on a worker pool:
 * those of one device on one stream one at a time, in the order sent, other streams in
 * parallel (spec §6); service calls (tool 0) at once. Other callbacks run on one callback
 * thread, training and history writes on their own threads: the sending path never waits for
 * them.
 */
class Node(dir: File, val name: String, private val timing: LearnTiming = LearnTiming()) {
    private val dir = dir.also { it.mkdirs() }
    val id: Identity = Identity.load(dir)
    val trust = Trust(dir)

    /**
     * onEvent receives tool events (except tool 0) from known devices: in the order sent on each
     * stream, one at a time, together with that stream's calls (spec §6), on a worker thread.
     */
    @Volatile var onEvent: ((peer: String, tool: Int, event: Int, args: List<Any?>) -> Unit)? = null
    /** onPaired is called after pairing (on either side). */
    @Volatile var onPaired: ((Peer) -> Unit)? = null
    /** onHello is called when a known device sends its hello. */
    @Volatile var onHello: ((String) -> Unit)? = null
    /** onHeld is called when messages from a device waited too long for a dictionary or its list and were dropped. */
    @Volatile var onHeld: ((peer: String, held: Int) -> Unit)? = null
    /** allowDict decides whether a known device may fetch a dictionary (0.5); null — all may. */
    @Volatile var allowDict: ((peer: String, id: ByteArray) -> Boolean)? = null
    /** noCompression turns compression off for what we send (benchmarks, debugging). */
    @Volatile var noCompression = false
    /** noLearning: do not train our own dictionaries (spec §5). */
    @Volatile var noLearning = false

    private val loop = ScheduledThreadPoolExecutor(1, daemon("p2pact-node"))
    private val workers: ExecutorService = Executors.newCachedThreadPool(daemon("p2pact-handler"))
    private val callbacks: ExecutorService = Executors.newSingleThreadExecutor(daemon("p2pact-callback"))
    private val io: ExecutorService = Executors.newSingleThreadExecutor(daemon("p2pact-io"))
    private val trainer: ExecutorService = Executors.newSingleThreadExecutor { r ->
        Thread(r, "p2pact-train").apply { isDaemon = true; priority = Thread.MIN_PRIORITY }
    }
    @Volatile private var loopThread: Thread? = null

    private val boot = u32(randomBytes(4), 0).let { if (it == 0L) 1L else it }
    private var udp: UdpRoad? = null
    private var tcp: TcpRoad? = null
    private var ownRelay: RelayHint? = null
    private val relays = HashMap<String, RelayRoad>() // by server address
    private val apps = LinkedHashSet<AttachedRoad>() // roads the application provides
    private var advertise: Hints? = null
    private var hintsSent: ByteArray? = null // our addresses as last announced (reannounce)
    private var hintsAt = 0L

    private val sessions = HashMap<Long, Session>() // by our index
    private val current = HashMap<String, Session>() // the best session with a device
    private val hs = HashMap<Long, PendingHs>()
    private val invites = HashMap<String, Invite>() // our invites by id (hex)
    private val peers = HashMap<String, PeerState>()
    private val handlers = HashMap<Pair<Int, Int>, Handler>()
    private val resetAt = HashMap<String, Long>()
    private val dicts = DictStore(dir, io)
    private val learner = Learner(dir, dicts, timing, io, trainer, ::post) { d -> addDictLocal(d) }
    private var woken = false
    private var paused = false // the device sleeps (setPaused): nothing is sent, nothing is taken in
    @Volatile private var closed = false
    private var lastTs = 0L
    private val handshakeCount = AtomicInteger()

    /** handshakes counts sessions set up (tests: a network change needs no new one). */
    val handshakes: Int get() = handshakeCount.get()

    init {
        loop.execute { loopThread = Thread.currentThread() }
        handle(0, 1) { _, _ -> emptyList() } // ping
        handle(0, 2) { peer, _ -> onLoop { info(peer) } }
        handle(0, 5) { peer, args -> dictHandler(peer, args) }
    }

    // --- the loop ---

    private fun post(f: () -> Unit) {
        if (closed) return
        try {
            loop.execute {
                try { f() } catch (e: Exception) { /* a malformed input is dropped */ }
            }
        } catch (e: java.util.concurrent.RejectedExecutionException) {
        }
    }

    /** onLoop runs f on the node's thread and waits for it. */
    private fun <T> onLoop(f: () -> T): T {
        if (Thread.currentThread() === loopThread) return f()
        val t = FutureTask(f)
        loop.execute(t)
        return try {
            t.get()
        } catch (e: java.util.concurrent.ExecutionException) {
            throw e.cause ?: e
        }
    }

    private fun receiveFromRoad(pkt: ByteArray, from: Endpoint) = post { receive(pkt, from) }

    /** listenUdp opens the main road on an address ("0.0.0.0:48730", "127.0.0.1:0"). */
    fun listenUdp(addr: String) {
        val u = UdpRoad(addr, ::receiveFromRoad)
        onLoop { udp = u }
    }

    /** rebindUdp handles a network change: listen again and tell known devices at once. Sessions stay. */
    fun rebindUdp(addr: String) {
        val u = UdpRoad(addr, ::receiveFromRoad)
        val old = onLoop {
            val old = udp
            udp = u
            val now = Clock.nanos()
            for ((peer, ps) in peers) {
                for ((k, e) in ps.eps.entries.toList()) {
                    if (old != null && e.ep.road === old) {
                        ps.eps.remove(k)
                        val ne = EpState(Endpoint(u, e.ep.addr)).also { it.lastRecv = e.lastRecv }
                        ps.eps[ne.ep.key] = ne
                    }
                }
                current[peer]?.let { sendData(now, peer, ps, it, keepalive(), true) }
            }
            old
        }
        old?.close()
    }

    /** listenTcp opens the fallback road; "" — dial only (a phone behind a carrier NAT can still reach a computer). */
    fun listenTcp(addr: String) {
        val t = TcpRoad(addr, ::receiveFromRoad)
        onLoop { tcp = t }
    }

    val udpRoad: UdpRoad? get() = onLoop { udp }
    val tcpRoad: TcpRoad? get() = onLoop { tcp }

    /** setRelay sets our relay: used for pairing and pair channels. */
    fun setRelay(h: RelayHint?) = onLoop { ownRelay = h }

    /** setAdvertise announces these hints instead of our own addresses; null — our own again. */
    fun setAdvertise(h: Hints?) = onLoop { advertise = h }

    /** start runs the service loop: delivery, retransmission, keepalives, connecting to known devices. */
    fun start() {
        loop.scheduleWithFixedDelay({ safe { pump(Clock.nanos()) } }, 20, 20, TimeUnit.MILLISECONDS)
        loop.scheduleWithFixedDelay({ safe { maintain(Clock.nanos()) } }, 1000, 1000, TimeUnit.MILLISECONDS)
        loop.scheduleWithFixedDelay({ safe { learner.tick() } }, timing.tickMs, timing.tickMs, TimeUnit.MILLISECONDS)
    }

    private inline fun safe(f: () -> Unit) {
        try { f() } catch (e: Exception) { }
    }

    fun close() {
        if (closed) return
        try {
            onLoop {
                closed = true
                learner.closed = true
                udp?.close()
                tcp?.close()
                for (r in relays.values) r.close()
                for (a in apps.toList()) a.detach()
                for (ps in peers.values) for (c in ps.calls.values) c.completeExceptionally(IllegalStateException("node closed"))
                for (h in hs.values) h.done?.completeExceptionally(IllegalStateException("node closed"))
            }
        } catch (e: Exception) {
        }
        loop.shutdown()
        workers.shutdown()
        callbacks.shutdown()
        trainer.shutdownNow()
        io.shutdown()
        try { io.awaitTermination(2, TimeUnit.SECONDS) } catch (e: InterruptedException) { }
        trust.flush()
    }

    /**
     * setPaused pauses the node (true) or resumes it: for a computer going to sleep, so that
     * nothing the node does on the network wakes it. While paused the node sends nothing at all and
     * takes in nothing: relay connections close, TCP connections close and new ones are refused
     * (the listener stays, so the port and the hints do not change), the UDP socket stays open but
     * silent, roads the application attached stay attached but get nothing to send. No keepalives,
     * no probes, no reconnects. What the application sends meanwhile is queued and calls keep their
     * timeouts. Other devices see a device that is unreachable.
     *
     * On resume the node speaks first: it rejoins its relay channels, sends a keepalive to every
     * device it has a session with (on all its roads) and starts a handshake with every other
     * known device, then sends what was queued. Calling it twice with the same value does nothing;
     * after [close] it does nothing.
     */
    fun setPaused(on: Boolean) {
        if (closed) return
        try {
            onLoop { if (!closed) pause(on) }
        } catch (e: java.util.concurrent.RejectedExecutionException) {
            // closed meanwhile
        }
    }

    /** relayChannels counts the relay channels open (tests). */
    internal val relayChannels: Int get() = onLoop { relays.values.sumOf { it.open } }

    private fun pause(on: Boolean) {
        if (paused == on) return
        paused = on
        tcp?.pause(on)
        if (on) {
            for (r in relays.values) r.close()
            for (a in apps) a.drop()
            return
        }
        val now = Clock.nanos()
        ownRelay?.let { h -> for (k in invites.keys) relay(h).join(k, "a") }
        for (p in trust.list()) {
            joinPair(p)
            peerState(p.addr)?.lastInit = 0 // the handshakes below go now, not after AUTO_CONNECT
        }
        for ((peer, ps) in peers) current[peer]?.let { sendData(now, peer, ps, it, keepalive(), true) }
        pump(now)
        maintain(now)
    }

    /**
     * handle registers method tool.method. Calls of one device on one stream run one at a time,
     * in the order sent, together with its events there (spec §6); other streams do not wait. A
     * handler that waits long returns [later], so that the stream goes on without it.
     */
    fun handle(tool: Int, method: Int, h: Handler) = onLoop { handlers[tool to method] = h }

    // --- pairing ---

    /** invite creates an invite valid for ttlMs; pairing is accepted on all our roads. */
    fun invite(ttlMs: Long): Invite = onLoop {
        val inv = Invite(id.key.public, name, randomBytes(32), System.currentTimeMillis() + ttlMs, hints())
        invites[inv.id.hex()] = inv
        if (!paused) ownRelay?.let { relay(it).join(inv.id.hex(), "a") } // paused: joined on resume
        inv
    }

    /** join pairs using an invite, trying all its hints until one answers. */
    fun joinAsync(inv: Invite, timeoutMs: Long = 30_000): CompletableFuture<Peer> {
        val done = CompletableFuture<Peer>()
        post {
            val eps = ArrayList<Endpoint>()
            udp?.let { u -> inv.hints.udp.forEach { eps.add(Endpoint(u, it)) } }
            tcp?.let { t -> inv.hints.tcp.forEach { eps.add(Endpoint(t, it)) } }
            var r: RelayRoad? = null
            inv.hints.relay?.let {
                r = relay(it)
                eps.add(Endpoint(r!!, inv.id.hex()))
            }
            // a link the application opened to a device it has not named
            val owner = Crypto.address(inv.key)
            for (a in apps) if (a.peer == null || a.peer == owner) eps.add(a.endpoint)
            if (eps.isEmpty()) {
                done.completeExceptionally(IllegalStateException("the invite has no road we can use"))
                return@post
            }
            val deadline = Clock.nanos() + timeoutMs * MS
            var local = 0L
            fun attempt() {
                hs.remove(local)
                if (done.isDone) return
                if (Clock.nanos() > deadline) {
                    done.completeExceptionally(TimeoutException("pairing timed out"))
                    return
                }
                val h = Handshake(Pattern.XXpsk2, true, PROLOGUE.toByteArray(), id.key, psk = inv.secret)
                val msg1 = h.writeMessage()
                local = newIndex()
                hs[local] = PendingHs(true, h, Clock.nanos(), invite = inv, done = done)
                val pkt = cat(byteArrayOf(P_PAIR1.toByte()), inv.id, be32(local), msg1)
                if (!paused) { // paused: try again on the next round
                    r?.join(inv.id.hex(), "b") // again after a pause closed it
                    for (e in eps) e.road.send(e.addr, pkt)
                }
                loop.schedule({ post { attempt() } }, INIT_EVERY / MS, TimeUnit.MILLISECONDS)
            }
            done.whenComplete { _, _ -> post { hs.remove(local); r?.leave(inv.id.hex()) } }
            attempt()
        }
        return done
    }

    /** join pairs using an invite and waits for it. */
    fun join(inv: Invite, timeoutMs: Long = 30_000): Peer = try {
        joinAsync(inv, timeoutMs).get()
    } catch (e: java.util.concurrent.ExecutionException) {
        throw e.cause ?: e
    }

    /** pairPayload is [name, boot, hints, signing key, os] in the pairing handshake. */
    private fun pairPayload(): ByteArray = Cbor.encode(listOf(name, boot, hints().toWire(), id.sign.public, goos()))

    /** befriend adds a device known from the family book without pairing: the pair secret comes from both keys. */
    fun befriend(p: Peer) {
        require(p.key.size == 32 && Crypto.address(p.key) == p.addr) { "key does not match address" }
        val old = trust.get(p.addr)
        if (old != null) {
            trust.put(old.copy(name = p.name, signKey = p.signKey, os = p.os))
            return
        }
        trust.put(p.copy(pairSecret = id.shared(p.key, "family-pair")))
    }

    /** forget drops a known device: we stop answering it. Our conversation's dictionaries and history go too. */
    fun forget(addr: String) = onLoop {
        dicts.forget(addr)
        learner.forget(addr)
        peers.remove(addr)?.let { ps -> for (c in ps.calls.values) c.completeExceptionally(IllegalStateException("device forgotten")) }
        current.remove(addr)?.let { sessions.remove(it.local) }
        trust.remove(addr)
    }

    // --- calls and events ---

    /**
     * call invokes tool.method on a known device and completes with the result elements. It
     * goes on stream 0: in order with every other stream-0 message. timeoutMs 0 — no timeout.
     */
    fun call(peer: String, tool: Int, method: Int, vararg args: Any?, timeoutMs: Long = 0): CompletableFuture<List<Any?>> =
        callOn(peer, 0, tool, method, *args, timeoutMs = timeoutMs)

    /**
     * callOn is call on a stream: in order with the stream's other messages, independent of other
     * streams (a loss on one does not hold up the rest). One stream per chat is natural; the
     * result comes back on the same stream.
     */
    fun callOn(peer: String, stream: Long, tool: Int, method: Int, vararg args: Any?, timeoutMs: Long = 0): CompletableFuture<List<Any?>> {
        val f = CompletableFuture<List<Any?>>()
        val msgArgs = args.toList()
        post {
            val ps = peerState(peer)
            if (ps == null) {
                f.completeExceptionally(IllegalArgumentException("unknown device"))
                return@post
            }
            val cid = ++ps.callID
            ps.calls[cid] = f
            if (tool == 0) ps.sysCalls.add(cid)
            f.whenComplete { _, _ -> post { ps.calls.remove(cid); ps.sysCalls.remove(cid) } }
            if (timeoutMs > 0) loop.schedule({ f.completeExceptionally(TimeoutException("call timed out")) }, timeoutMs, TimeUnit.MILLISECONDS)
            push(ps, Cbor.encode(listOf<Any?>(0, cid, tool, method) + msgArgs), stream, tool != 0)
        }
        if (closed) f.completeExceptionally(IllegalStateException("node closed"))
        return f
    }

    /** event sends event tool.event to a known device on stream 0 (in order, like calls). */
    fun event(peer: String, tool: Int, event: Int, vararg args: Any?) = eventOn(peer, 0, tool, event, *args)

    /** eventOn is event on a stream (see callOn). */
    fun eventOn(peer: String, stream: Long, tool: Int, event: Int, vararg args: Any?) {
        val msg = Cbor.encode(listOf<Any?>(3, tool, event) + args.toList())
        onLoop {
            val ps = peerState(peer) ?: throw IllegalArgumentException("unknown device")
            push(ps, msg, stream, tool != 0)
        }
    }

    /** paths lists live roads to a known device: udp, tcp, relay:<address>. */
    fun paths(peer: String): List<String> = onLoop {
        val ps = peers[peer] ?: return@onLoop emptyList()
        val now = Clock.nanos()
        ps.eps.values.filter { now - it.lastRecv <= ALIVE_FOR }.map { it.ep.road.name }.distinct()
    }

    /** info is the hello (spec §6, 0.2): [name, os, implementation, {tool: version}, [dictionaries], hints]. */
    private fun info(peer: String): List<Any?> {
        val tools = LinkedHashMap<Any?, Any?>()
        for (k in handlers.keys) if (k.first != 0) tools[k.first] = 1
        return listOf(name, goos(), "p2pact-kotlin", tools, dicts.ids(peer), hints().toWire())
    }

    private fun hints(): Hints {
        advertise?.let { return if (it.relay == null && ownRelay != null) it.copy(relay = ownRelay) else it }
        return Hints(udp = udp?.localAddrs() ?: emptyList(), tcp = tcp?.localAddrs() ?: emptyList(), relay = ownRelay)
    }

    // --- receiving ---

    private fun receive(pkt: ByteArray, from: Endpoint) {
        // paused: what came is dropped unanswered, its sender sends it again after we resume
        if (pkt.size < 5 || closed || paused) return
        val now = Clock.nanos()
        try {
            when (pkt[0].toInt()) {
                P_DATA -> onData(now, pkt, from)
                P_INIT -> onInit(now, pkt, from)
                P_RESP -> onResp(now, pkt, from)
                P_PAIR1 -> onPair1(now, pkt, from)
                P_PAIR2 -> onPair2(now, pkt, from)
                P_PAIR3 -> onPair3(now, pkt, from)
                P_RESET -> {
                    val idx = u32(pkt, 1)
                    for (s in sessions.values.toList()) {
                        if (s.remote == idx) {
                            sessions.remove(s.local)
                            if (current[s.peer] === s) current.remove(s.peer)
                        }
                    }
                }
            }
        } catch (e: NoiseException) {
            // a stranger or a damaged packet: silence
        } catch (e: RuntimeException) {
        }
    }

    private fun onData(now: Long, pkt: ByteArray, from: Endpoint) {
        if (pkt.size < DATA_HEADER + 16) return
        val idx = u32(pkt, 1)
        val s = sessions[idx]
        if (s == null) {
            // our handshake is still waiting for its reply, and data from the new session
            // overtook it: hold the data instead of resetting the session
            val h = hs[idx]
            if (h != null && !h.pair && h.early.size < 16) {
                h.early.add(pkt to from)
                return
            }
            val k = from.key
            if (now - (resetAt[k] ?: 0L) >= 5 * SEC) {
                resetAt[k] = now
                from.road.send(from.addr, cat(byteArrayOf(P_RESET.toByte()), be32(idx)))
            }
            return
        }
        val cnt = u32(pkt, 5)
        val plain = s.recv.decryptAt(cnt, pkt.copyOf(DATA_HEADER), pkt, DATA_HEADER) ?: return
        if (!s.win.accept(cnt)) return
        s.lastRecv = now
        if (current[s.peer] == null) current[s.peer] = s
        val ps = peerState(s.peer) ?: return
        ps.heard = true
        // a packet on a better road than the one in use (a probe, spec §7) is answered on it: the
        // other side hears the road too, and both move to it
        val c = candidates(s.peer, ps, now, false)
        val better = c.size == 1 && roadRank(from.road) < roadRank(c[0].ep.road)
        seen(ps, from, now, s.local)
        if (better) sealTo(s, ps.eps[from.key]!!, now, keepalive())
        ps.st.receive(now, plain) { m, z, st -> inbound(s.peer, m, z, st) }
        if (ps.st.ackDue != 0L && now >= ps.st.ackDue) wake() // an ack is due now
    }

    /** onInit: a known device starts a session (Noise_IK) with data in the first packet (spec §4.2). */
    private fun onInit(now: Long, pkt: ByteArray, from: Endpoint) {
        val h = Handshake(Pattern.IK, false, PROLOGUE.toByteArray(), id.key)
        val payload = h.readMessage(pkt.copyOfRange(5, pkt.size))
        if (payload.size < 12) return
        val peer = Crypto.address(h.rs!!)
        if (trust.get(peer) == null) return // silence to strangers
        val ps = peerState(peer) ?: return
        val ts = u64(payload, 0)
        val age = System.currentTimeMillis() * MS - ts
        if (ts <= ps.lastTS || age > TS_SKEW || age < -TS_SKEW) return // a replay of an old start
        ps.lastTS = ts
        val local = newIndex()
        val resp = h.writeMessage(be32(boot))
        val (c1, c2) = h.ciphers ?: return
        val s = Session(peer, local, u32(pkt, 1), c2, c1, now)
        sessions[local] = s
        current[peer] = s
        handshakeCount.incrementAndGet()
        seen(ps, from, now, s.local)
        bootOf(ps, u32(payload, 8))
        from.road.send(from.addr, cat(byteArrayOf(P_RESP.toByte()), be32(s.remote), be32(local), resp))
        // data in the first packet; from a device that has not heard our boot id it may be
        // numbered for our previous boot, to be renumbered once it gets our reply: drop it
        // unacknowledged, it comes again in the session (spec §4.2)
        if (payload.size > 12 && ps.heard) {
            ps.st.receive(now, payload.copyOfRange(12, payload.size)) { m, z, st -> inbound(peer, m, z, st) }
        }
        sendInfo(peer)
    }

    private fun onResp(now: Long, pkt: ByteArray, from: Endpoint) {
        if (pkt.size < 9) return
        val local = u32(pkt, 1)
        val h = hs[local] ?: return
        if (h.pair) return
        val payload = h.hs.readMessage(pkt.copyOfRange(9, pkt.size))
        val (c1, c2) = h.hs.ciphers ?: return
        if (payload.size < 4) return
        hs.remove(local)
        val s = Session(h.peer, local, u32(pkt, 5), c1, c2, now)
        sessions[local] = s
        current[h.peer] = s
        handshakeCount.incrementAndGet()
        val ps = peerState(h.peer) ?: return
        ps.heard = true
        seen(ps, from, now, s.local)
        bootOf(ps, u32(payload, 0))
        sendInfo(h.peer)
        for ((p, f) in h.early) onData(now, p, f)
    }

    /** onPair1: someone joins with our invite (Noise_XXpsk2, we respond). */
    private fun onPair1(now: Long, pkt: ByteArray, from: Endpoint) {
        if (pkt.size < 21) return
        val inv = invites[pkt.copyOfRange(1, 17).hex()] ?: return
        if (System.currentTimeMillis() > inv.exp) return
        val h = Handshake(Pattern.XXpsk2, false, PROLOGUE.toByteArray(), id.key, psk = inv.secret)
        h.readMessage(pkt.copyOfRange(21, pkt.size))
        val msg2 = h.writeMessage(pairPayload())
        val local = newIndex()
        val remoteIdx = u32(pkt, 17)
        hs[local] = PendingHs(true, h, now, invite = inv, remoteIdx = remoteIdx)
        from.road.send(from.addr, cat(byteArrayOf(P_PAIR2.toByte()), be32(remoteIdx), be32(local), msg2))
    }

    /** onPair2: we are joining; the invite owner answered. */
    private fun onPair2(now: Long, pkt: ByteArray, from: Endpoint) {
        if (pkt.size < 9) return
        val local = u32(pkt, 1)
        val h = hs[local] ?: return
        if (!h.pair || h.done == null) return
        val payload = try { h.hs.readMessage(pkt.copyOfRange(9, pkt.size)) } catch (e: NoiseException) { return } // wrong secret or a stranger
        val inv = h.invite!!
        if (!same(h.hs.rs!!, inv.key)) {
            hs.remove(local)
            h.done.completeExceptionally(IllegalStateException("device key does not match the invite"))
            return
        }
        val pp = try { Cbor.decode(payload).asList() } catch (e: Exception) { null } ?: emptyList()
        val msg3 = h.hs.writeMessage(pairPayload())
        val (c1, c2) = h.hs.ciphers ?: return
        hs.remove(local)
        val peer = Crypto.address(inv.key)
        val wh = Hints.fromWire(pp.getOrNull(2))
        val hints = Hints(
            udp = wh.udp.ifEmpty { inv.hints.udp }, tcp = wh.tcp.ifEmpty { inv.hints.tcp }, relay = wh.relay ?: inv.hints.relay,
        )
        val p = Peer(peer, inv.key, pp.getOrNull(0).asString() ?: "", inv.secret, hints,
            signKey = pp.getOrNull(3).asBytes(), os = pp.getOrNull(4).asString())
        trust.put(p)
        val s = Session(peer, local, u32(pkt, 5), c1, c2, now)
        sessions[local] = s
        current[peer] = s
        val ps = peerState(peer) ?: return
        seen(ps, from, now, s.local)
        bootOf(ps, pp.getOrNull(1).asLong() ?: 0)
        from.road.send(from.addr, cat(byteArrayOf(P_PAIR3.toByte()), be32(s.remote), msg3))
        paired(trust.get(peer) ?: p)
        h.done.complete(trust.get(peer) ?: p)
    }

    /** onPair3: the joiner confirmed; pairing is complete. */
    private fun onPair3(now: Long, pkt: ByteArray, from: Endpoint) {
        val local = u32(pkt, 1)
        val h = hs[local] ?: return
        if (!h.pair || h.done != null) return
        val payload = h.hs.readMessage(pkt.copyOfRange(5, pkt.size))
        val (c1, c2) = h.hs.ciphers ?: return
        hs.remove(local)
        val inv = h.invite!!
        invites.remove(inv.id.hex()) // single use
        ownRelay?.let { relay(it).leave(inv.id.hex()) }
        val key = h.hs.rs!!.copyOf()
        val peer = Crypto.address(key)
        val pp = try { Cbor.decode(payload).asList() } catch (e: Exception) { null } ?: emptyList()
        val p = Peer(peer, key, pp.getOrNull(0).asString() ?: "", inv.secret, Hints.fromWire(pp.getOrNull(2)),
            signKey = pp.getOrNull(3).asBytes(), os = pp.getOrNull(4).asString(), via = inv.id.hex())
        trust.put(p)
        // the joiner was the initiator; we are the responder
        val s = Session(peer, local, h.remoteIdx, c2, c1, now)
        sessions[local] = s
        current[peer] = s
        val ps = peerState(peer) ?: return
        seen(ps, from, now, s.local)
        bootOf(ps, pp.getOrNull(1).asLong() ?: 0)
        paired(trust.get(peer) ?: p)
    }

    private fun paired(p: Peer) {
        sendInfo(p.addr)
        onPaired?.let { cb -> callbacks.execute { cb(p) } }
    }

    /** bootOf records the other device's boot id; a change means its receiving restarted. */
    private fun bootOf(ps: PeerState, boot: Long) {
        if (ps.boot != 0L && ps.boot != boot) ps.st.reset()
        ps.boot = boot
    }

    /** sendInfo sends our hello: name, where to find us and our dictionaries (event 0.2). */
    private fun sendInfo(peer: String) {
        val ps = peerState(peer) ?: return
        hintsSent = Cbor.encode(hints().toWire()) // what the devices know of us (reannounce)
        push(ps, Cbor.encode(listOf<Any?>(3, 0, 2) + info(peer)), 0, false)
    }

    /** message handles a reassembled message (spec §6). */
    private fun message(peer: String, msg: ByteArray, stream: Long) {
        val a = try { Cbor.decode(msg).asList() } catch (e: Exception) { null } ?: return
        if (a.size < 2) return
        val ps = peerState(peer) ?: return
        when (a[0].asLong()) {
            0L -> { // call
                if (a.size < 4) return
                val cid = a[1].asLong() ?: return
                val tool = a[2].asInt() ?: return
                val method = a[3].asInt() ?: return
                if (tool != 0) hear(peer, msg)
                val h = handlers[tool to method]
                val args = a.subList(4, a.size)
                val boot = ps.boot
                fun reply(res: List<Any?>?, err: Throwable?) {
                    val e = if (err is java.util.concurrent.CompletionException && err.cause != null) err.cause else err
                    val out: List<Any?> = when (e) {
                        null -> listOf<Any?>(1, cid) + res.orEmpty()
                        is P2pactError -> listOf(2, cid, e.code, e.message ?: "")
                        else -> listOf(2, cid, Errors.FAILED, e.message ?: e.toString())
                    }
                    val b = Cbor.encode(out)
                    post {
                        // the result goes back on the call's stream, unless the caller restarted meanwhile
                        val p = peerState(peer)
                        if (p != null && (boot == 0L || p.boot == boot)) {
                            // a hello, like the event: never compressed (spec §5)
                            if (tool == 0 && method == 2) { p.st.push(b, false, stream, true); wake() }
                            else push(p, b, stream, tool != 0, reply = true)
                        }
                    }
                }
                val run: () -> Unit = {
                    if (h == null) {
                        reply(null, P2pactError(Errors.NO_METHOD, "no such method"))
                    } else {
                        val res = try { h.handle(peer, args) } catch (e: Exception) { reply(null, e); null }
                        // a reply given later: the turn ends here, the reply goes when it is ready
                        if (res is Later) res.reply.whenComplete { r, e -> reply(r, e) }
                        else if (res != null) reply(res, null)
                    }
                }
                // service calls (a dictionary, a ping) never wait behind a slow handler
                if (tool == 0) execute(run) else enqueue(ps, stream, run)
            }
            1L, 2L -> { // result, error
                val cid = a[1].asLong() ?: return
                val f = ps.calls[cid] ?: return
                if (!ps.sysCalls.contains(cid)) hear(peer, msg)
                ps.calls.remove(cid)
                if (a[0] == 1L) {
                    f.complete(a.subList(2, a.size))
                } else {
                    f.completeExceptionally(P2pactError(a.getOrNull(2).asInt() ?: Errors.FAILED, a.getOrNull(3).asString() ?: ""))
                }
            }
            3L -> { // event
                if (a.size < 3) return
                val tool = a[1].asInt() ?: return
                val ev = a[2].asInt() ?: return
                if (tool == 0 && ev == 2 && a.size >= 9) { // hello: name, hints, dictionaries
                    trust.setHints(peer, a[3].asString() ?: "", a[4].asString() ?: "", Hints.fromWire(a[8]))
                    ps.dicts = idsOf(a[7])
                    ps.lacks = ArrayList()
                    ps.tried = ArrayList()
                    fetchNew(peer, ps)
                    drain(peer, ps)
                    onHello?.let { cb -> callbacks.execute { cb(peer) } }
                    return
                }
                if (tool == 0 && ev == 5 && a.size >= 4) { // the dictionary list changed
                    ps.dicts = idsOf(a[3])
                    ps.lacks = ArrayList()
                    ps.tried = ArrayList()
                    fetchNew(peer, ps)
                    drain(peer, ps)
                    return
                }
                if (tool != 0) hear(peer, msg)
                val cb = onEvent ?: return
                val args = a.subList(3, a.size)
                enqueue(ps, stream) { cb(peer, tool, ev, args) }
            }
        }
    }

    private fun execute(f: () -> Unit) {
        try {
            workers.execute(f)
        } catch (e: java.util.concurrent.RejectedExecutionException) { // closed
        }
    }

    /**
     * enqueue runs f behind the stream's earlier calls and events. Each stream has its own turn
     * on the worker pool while it has work: a slow handler holds up its stream only.
     */
    private fun enqueue(ps: PeerState, stream: Long, f: () -> Unit) {
        val ib = ps.inbox.getOrPut(stream) { Inbox() }
        ib.q.addLast(f)
        if (ib.running) return
        ib.running = true
        runNext(ps, stream, ib)
    }

    private fun runNext(ps: PeerState, stream: Long, ib: Inbox) {
        val f = ib.q.removeFirstOrNull()
        if (f == null || closed) {
            ib.running = false
            if (ps.inbox[stream] === ib) ps.inbox.remove(stream)
            return
        }
        execute {
            try {
                f()
            } catch (e: Exception) { // a failing callback does not stop its stream
            } finally {
                post { runNext(ps, stream, ib) }
            }
        }
    }

    // --- sending ---

    private fun peerState(peer: String): PeerState? {
        peers[peer]?.let { return it }
        if (trust.get(peer) == null) return null
        return PeerState(peer).also { peers[peer] = it }
    }

    private fun seen(ps: PeerState, from: Endpoint, now: Long, session: Long) {
        val e = ps.eps.getOrPut(from.key) { EpState(from) }
        e.lastRecv = now
        e.session = session
    }

    /** candidates picks where to send: the best live road; with none alive or on retransmission, all hints too. */
    private fun candidates(peer: String, ps: PeerState, now: Long, all: Boolean): List<EpState> {
        var alive = ps.eps.values.filter { now - it.lastRecv <= ALIVE_FOR }
        // prefer roads the current session was heard on: earlier sessions may have used roads that are gone now
        val cur = current[peer]
        if (cur != null && !all) {
            val mine = alive.filter { it.session == cur.local }
            if (mine.isNotEmpty()) alive = mine
        }
        if (alive.isNotEmpty() && !all) {
            // the road heard from last; a better kind only if heard within half a second of it (spec §7)
            var best = alive.maxByOrNull { it.lastRecv }!!
            for (e in alive) if (roadRank(e.ep.road) < roadRank(best.ep.road) && best.lastRecv - e.lastRecv < 500 * MS) best = e
            return listOf(best)
        }
        val out = ArrayList(alive)
        val p = trust.get(peer) ?: return out
        fun add(ep: Endpoint) {
            val e = ps.eps.getOrPut(ep.key) { EpState(ep) }
            if (out.none { it === e }) out.add(e)
        }
        udp?.let { u -> p.hints.udp.forEach { add(Endpoint(u, it)) } }
        tcp?.let { t -> p.hints.tcp.forEach { add(Endpoint(t, it)) } }
        pairRelay(p)?.let { add(Endpoint(it, p.relayChannel)) }
        for (a in apps) if (a.peer == peer) add(a.endpoint) // a road the application opened to this device
        return out
    }

    /** roadRank orders kinds of roads (spec §7): UDP, TCP, a relay, then a road the application provides. */
    private fun roadRank(r: Road): Int = if (r is AttachedRoad) 3 else when (r.name) { "udp" -> 0; "tcp" -> 1; else -> 2 }

    /** sealTo sends packet content on one endpoint only (a keepalive, a probe). */
    private fun sealTo(s: Session, e: EpState, now: Long, plain: ByteArray) {
        if (paused) return
        if (e.ep.road.send(e.ep.addr, seal(s, plain))) e.lastSend = now
    }

    /**
     * probe tries the direct roads to a device while the session runs over a relay or a road the
     * application provides (spec §7): a keepalive to every UDP and TCP address known for it, every
     * [probeEvery]. The one that answers is live, and being better it takes over.
     */
    private fun probe(now: Long, peer: String, ps: PeerState, s: Session) {
        if (now - ps.lastProbe < probeEvery) return
        val c = candidates(peer, ps, now, false)
        if (c.size != 1 || roadRank(c[0].ep.road) < 2) return
        ps.lastProbe = now
        for (e in candidates(peer, ps, now, true)) if (roadRank(e.ep.road) < 2) sealTo(s, e, now, keepalive())
    }

    // --- application roads ---

    /**
     * attachRoad attaches a road the application provides (spec §3): Bluetooth LE GATT, a serial
     * line, a pipe. peer is the device it reaches, when the application knows it (it found that
     * device's Bluetooth advertisement, say): the node sends to it on this road like on its hints,
     * and starts a session over it at once if it has none. With peer null the road is bound by what
     * arrives on it: a handshake or session packet names the device, and the answers go back the
     * same way; until then it is offered for pairing ([join]). The road ranks below UDP, TCP and a
     * relay: it carries the session while nothing better is up, and the session moves to a better
     * road as soon as one answers (spec §7).
     */
    fun attachRoad(peer: String?, road: AppRoad, config: AppRoadConfig): AttachedRoad {
        require(config.mtu >= 1) { "an application road needs an MTU" }
        require(config.stream || config.mtu >= MAX_ROAD_PACKET) {
            "a datagram road must carry $MAX_ROAD_PACKET bytes; a smaller link that keeps order is a stream road"
        }
        if (peer != null && trust.get(peer) == null) throw IllegalArgumentException("unknown device")
        val a = AttachedRoad(road, config, peer, ::receiveFromRoad) { r -> post { detached(r) } }
        onLoop {
            apps.add(a)
            if (peer != null) appAttached(Clock.nanos(), a)
        }
        return a
    }

    /**
     * appAttached starts using a road the application attached for a device: a keepalive over it
     * when there is a session (so the other side binds it too), a handshake when not.
     */
    private fun appAttached(now: Long, a: AttachedRoad) {
        if (paused) return // it is offered on resume, with the other roads
        val peer = a.peer ?: return
        val ps = peerState(peer) ?: return
        val e = ps.eps.getOrPut(a.endpoint.key) { EpState(a.endpoint) }
        val s = current[peer]
        if (s != null) {
            sealTo(s, e, now, keepalive())
            return
        }
        ps.lastInit = 0
        startInit(now, peer, ps, if (ps.st.idle()) null else ps.st.nextPacket(now, false)?.plain)
    }

    private fun detached(a: AttachedRoad) {
        apps.remove(a)
        for (ps in peers.values) ps.eps.entries.removeIf { it.value.ep.road === a }
    }

    /** pairRelay picks the pair's relay: the one of the lower address, else the other's. */
    private fun pairRelay(p: Peer): RelayRoad? {
        val mine = ownRelay
        val theirs = p.hints.relay
        val (first, second) = if (p.addr < id.addr) theirs to mine else mine to theirs
        val h = first ?: second ?: return null
        return relay(h)
    }

    private fun relay(h: RelayHint): RelayRoad = relays.getOrPut(h.addr) { RelayRoad(h, ::receiveFromRoad) }

    private fun sendData(now: Long, peer: String, ps: PeerState, s: Session, plain: ByteArray, all: Boolean) {
        if (paused) return
        val pkt = seal(s, plain)
        for (e in candidates(peer, ps, now, all)) if (e.ep.road.send(e.ep.addr, pkt)) e.lastSend = now
    }

    private fun seal(s: Session, plain: ByteArray): ByteArray {
        val hdr = cat(byteArrayOf(P_DATA.toByte()), be32(s.remote), be32(s.sendN))
        val pkt = cat(hdr, s.send.encryptAt(s.sendN, hdr, plain))
        s.sendN++
        return pkt
    }

    /** startInit starts a session with a known device (Noise_IK); first is first-packet data. */
    private fun startInit(now: Long, peer: String, ps: PeerState, first: ByteArray?) {
        val p = trust.get(peer) ?: return
        if (paused || now - ps.lastInit < ps.initGap()) return
        ps.lastInit = now
        // keep the last few unanswered starts: a reply to any of them opens the session
        val pending = hs.entries.filter { !it.value.pair && it.value.peer == peer }.sortedBy { it.value.at }.map { it.key }.toMutableList()
        while (pending.size >= 3) hs.remove(pending.removeAt(0))
        val h = Handshake(Pattern.IK, true, PROLOGUE.toByteArray(), id.key, rs = p.key)
        lastTs = maxOf(lastTs + 1, System.currentTimeMillis() * MS) // strictly newer than the last start
        val msg1 = h.writeMessage(cat(be64(lastTs), be32(boot), first ?: ByteArray(0)))
        val local = newIndex()
        hs[local] = PendingHs(false, h, now, peer = peer)
        val pkt = cat(byteArrayOf(P_INIT.toByte()), be32(local), msg1)
        for (e in candidates(peer, ps, now, true)) if (e.ep.road.send(e.ep.addr, pkt)) e.lastSend = now
    }

    private fun wake() {
        if (woken || closed) return
        woken = true
        post {
            woken = false
            pump(Clock.nanos())
        }
    }

    /** pump sends what is queued and what is due for retransmission. */
    private fun pump(now: Long) {
        if (paused) return // nothing goes, and no timer counts a try: delivery resumes where it stopped
        for ((peer, ps) in peers) {
            val s = current[peer]
            if (s == null) {
                // take a part for the first packet only when a handshake really goes out (a
                // restarted device drops it unacknowledged until it has heard us: spec §4.2)
                if (!ps.st.idle() && now - ps.lastInit >= ps.initGap() && trust.get(peer) != null) {
                    startInit(now, peer, ps, ps.st.nextPacket(now, false)?.plain)
                }
                continue
            }
            // nothing for 10 s while retransmitting: the session looks dead, start a new one
            if (!ps.st.idle() && now - s.lastRecv > 10 * SEC && ps.st.stuck()) {
                current.remove(peer)
                continue
            }
            val c = candidates(peer, ps, now, false)
            if (c.size == 1) {
                val r = c[0].ep.road
                ps.st.reliable = if (r is AttachedRoad) r.config.stream else r.name != "udp"
                ps.st.link = if (r is AttachedRoad) r.rate else 0.0
            }
            var i = 0
            while (true) {
                if (i++ == 256) { // a burst at a time; the rest on the next wake
                    wake()
                    break
                }
                val next = ps.st.nextPacket(now, false) ?: break
                sendData(now, peer, ps, s, next.plain, next.retry)
            }
            for (e in ps.eps.values) { // keepalive on every quiet road
                if (now - e.lastRecv <= ALIVE_FOR && now - e.lastSend >= keepaliveGap) {
                    if (e.ep.road.send(e.ep.addr, seal(s, keepalive()))) e.lastSend = now
                }
            }
        }
    }

    /** maintain runs every second: connect to known devices, keep relay channels, clean up. */
    private fun maintain(now: Long) {
        if (paused) return
        for (p in trust.list()) {
            val ps = peerState(p.addr) ?: continue
            joinPair(p)
            val s = current[p.addr]
            if (s == null && now - ps.lastInit >= AUTO_CONNECT) startInit(now, p.addr, ps, null)
            if (s != null) probe(now, p.addr, ps, s)
        }
        unhold(now)
        reannounce(now)
        sessions.entries.removeIf { (_, s) -> current[s.peer] !== s && now - s.lastRecv > SESSION_IDLE }
        hs.entries.removeIf { now - it.value.at > 60 * SEC }
        val wall = System.currentTimeMillis()
        for ((k, inv) in invites.entries.toList()) {
            if (wall > inv.exp) {
                invites.remove(k)
                ownRelay?.let { relay(it).leave(k) }
            }
        }
    }

    /** joinPair keeps the pair's relay channel open. */
    private fun joinPair(p: Peer) {
        pairRelay(p)?.join(p.relayChannel, if (p.addr < id.addr) "b" else "a")
    }

    private fun newIndex(): Long {
        while (true) {
            val i = u32(randomBytes(4), 0)
            if (i != 0L && !sessions.containsKey(i) && !hs.containsKey(i)) return i
        }
    }

    // --- compression (spec §5) ---

    /**
     * push queues a message, compressed when worth it: with our dictionary for this device, else
     * the newest one it also has. The slot refers to our list at send time: the list changes only
     * through event 0.5, which travels in the same order as messages. talk: the message is
     * conversation (not tool 0), so it goes to the history.
     */
    private fun push(ps: PeerState, msg: ByteArray, stream: Long, talk: Boolean, reply: Boolean = false) {
        if (noCompression || bootstrap(msg)) {
            ps.st.push(msg, false, stream, reply)
        } else {
            val (d, slot) = dicts.pick(ps.addr, ps.dicts, ps.lacks)
            val z = compress(msg, d, slot)
            if (talk && !noLearning) learner.talk(ps.addr, msg, d, z)
            if (z != null) ps.st.push(z, true, stream, reply) else ps.st.push(msg, false, stream, reply)
        }
        wake()
    }

    /** hear queues a conversation message we got from the device for the history. */
    private fun hear(peer: String, msg: ByteArray) {
        if (!noCompression && !noLearning) learner.hear(peer, msg)
    }

    /**
     * inbound decompresses a reassembled message and runs it in order. A missing dictionary is not
     * an error: we hold the message and everything after it and ask the sender for it; if we do
     * not know the sender's list yet (it restarted), we wait for its hello. While waiting, only
     * results of our calls, hellos and dictionary lists run out of order.
     */
    private fun inbound(peer: String, msg: ByteArray, zip: Boolean, stream: Long) {
        val ps = peerState(peer) ?: return
        if (ps.waiting.isNotEmpty()) {
            val r = unzip(ps, msg, zip)
            if (r is Unzip.Plain && outOfOrder(r.msg)) {
                message(peer, r.msg, stream)
                drain(peer, ps)
                return
            }
        }
        if (ps.waiting.isEmpty()) ps.waitSince = Clock.nanos()
        ps.waiting.addLast(Inbound(msg, zip, stream))
        drain(peer, ps)
    }

    /**
     * unhold: held messages do not wait forever (spec §5) — after HOLD_ASK we ask again (the hello,
     * the dictionary); after HOLD_DROP they are dropped (the sender resends what matters) and the
     * lists are learnt anew.
     */
    private fun unhold(now: Long) {
        for ((peer, ps) in peers) {
            if (ps.waiting.isEmpty()) continue
            val held = now - ps.waitSince
            when {
                held >= holdDrop -> {
                    val k = ps.waiting.size
                    ps.waiting.clear()
                    ps.fetching = false; ps.askingInfo = false
                    ps.dicts = emptyList(); ps.lacks = ArrayList(); ps.tried = ArrayList()
                    sendInfo(peer)
                    onHeld?.let { cb -> io.execute { cb(peer, k) } }
                }
                held >= holdAsk -> if (ps.dicts.isEmpty()) askInfo(peer, ps) else { ps.tried = ArrayList(); drain(peer, ps) }
            }
        }
    }

    /**
     * reannounce: our addresses changed (another network, an advertised address) — tell the devices
     * in session at once with our hello, not at the next handshake (as p2pact-go).
     */
    private fun reannounce(now: Long) {
        if (now - hintsAt < 5 * SEC) return
        hintsAt = now
        val sent = hintsSent ?: return
        val cur = Cbor.encode(hints().toWire())
        if (cur.contentEquals(sent)) return
        for (peer in current.keys.toList()) sendInfo(peer)
    }

    /** askInfo asks the device for its hello (0.2 info) when we hold its messages without its list. */
    private fun askInfo(peer: String, ps: PeerState) {
        val now = Clock.nanos()
        if (ps.askingInfo || now - ps.askedInfo < 10 * SEC) return
        ps.askingInfo = true; ps.askedInfo = now
        call(peer, 0, 2, timeoutMs = 30_000).whenComplete { res, err ->
            post {
                ps.askingInfo = false
                if (err != null || res == null || res.size < 5 || ps.dicts.isNotEmpty() || peers[peer] !== ps) return@post
                ps.dicts = idsOf(res[4])
                fetchNew(peer, ps)
                drain(peer, ps)
            }
        }
    }

    /** drain runs waiting messages in order until one cannot be decoded yet. */
    private fun drain(peer: String, ps: PeerState) {
        while (ps.waiting.isNotEmpty()) {
            val w = ps.waiting.first()
            when (val r = unzip(ps, w.msg, w.zip)) {
                Unzip.Missing -> {
                    val did = ps.dicts[(w.msg[0].toInt() and 0xff) - 1]
                    if (did.isNotEmpty()) {
                        fetchDict(peer, ps, did, true)
                        return
                    }
                    ps.waiting.removeFirst() // a free slot: the message is damaged
                }
                Unzip.NoList -> { askInfo(peer, ps); return } // wait for a hello or 0.5, and ask for it
                Unzip.Bad -> ps.waiting.removeFirst()
                is Unzip.Plain -> {
                    ps.waiting.removeFirst()
                    message(peer, r.msg, w.stream)
                }
            }
        }
    }

    private sealed class Unzip {
        class Plain(val msg: ByteArray) : Unzip()
        object Missing : Unzip() // we lack the dictionary in that slot
        object NoList : Unzip() // the slot is beyond the list we know
        object Bad : Unzip()
    }

    private fun unzip(ps: PeerState, msg: ByteArray, zip: Boolean): Unzip {
        if (!zip) return Unzip.Plain(msg)
        if (msg.isEmpty()) return Unzip.Bad
        var d: Dict? = null
        val slot = msg[0].toInt() and 0xff
        if (slot > 0) {
            if (slot > ps.dicts.size) return Unzip.NoList
            d = dicts.get(ps.dicts[slot - 1], ps.addr) ?: return Unzip.Missing
        }
        return try { Unzip.Plain(decompress(msg, d)) } catch (e: Exception) { Unzip.Bad }
    }

    /** fetchNew asks the device for the newest dictionary in its list that we lack: so it can compress with it. */
    private fun fetchNew(peer: String, ps: PeerState) {
        val did = ps.dicts.firstOrNull { it.isNotEmpty() && dicts.get(it, peer) == null && !hasId(ps.tried, it) } ?: return
        fetchDict(peer, ps, did, false)
    }

    /**
     * fetchDict asks the device for dictionary id (0.5 dict): one it compressed a waiting message
     * with, or a new one in its list. What we get belongs to the pair with it.
     */
    private fun fetchDict(peer: String, ps: PeerState, did: ByteArray, waiting: Boolean) {
        if (ps.fetching) return
        ps.fetching = true
        fun attempt(tryN: Int) {
            call(peer, 0, 5, did, timeoutMs = 30_000).whenComplete { res, err ->
                post {
                    val raw = if (err == null) res?.getOrNull(0).asBytes() else null
                    var d: Dict? = if (raw != null && same(dictId(raw), did)) {
                        try { Dict(raw, peer, false) } catch (e: Exception) { null }
                    } else null
                    if (d == null && tryN < 2 && !closed && peers[peer] === ps) {
                        attempt(tryN + 1)
                        return@post
                    }
                    ps.fetching = false
                    if (closed || peers[peer] !== ps) return@post // forgotten meanwhile
                    if (d != null && !addDictLocal(d)) d = null // could not store it: do not ask again and again
                    if (d == null) {
                        if (waiting && ps.waiting.isNotEmpty()) ps.waiting.removeFirst() // unreachable: this message is lost
                        else ps.tried.add(did)
                    }
                    drain(peer, ps)
                    fetchNew(peer, ps)
                }
            }
        }
        attempt(0)
    }

    /**
     * dictHandler serves 0.5 dict [id] → dictionary bytes, only one the asker may see. A device that
     * asks lacks it, so we stop compressing with it for that device (this reply included) until it
     * announces again.
     */
    private fun dictHandler(peer: String, args: List<Any?>): List<Any?> {
        val did = args.getOrNull(0).asBytes() ?: throw P2pactError(Errors.BAD_ARGS, "dictionary id required")
        allowDict?.let { if (!it(peer, did)) throw P2pactError(Errors.FORBIDDEN, "dictionary is for the family only") }
        val d = onLoop {
            peers[peer]?.lacks?.add(did)
            dicts.get(did, peer)
        } ?: throw P2pactError(Errors.NOT_FOUND, "no such dictionary")
        return listOf(d.raw)
    }

    /** addDict stores a new (newest) common dictionary — [CommonDict.raw], say — and announces it (event 0.5). */
    fun addDict(raw: ByteArray) {
        val d = Dict(raw)
        if (!onLoop { addDictLocal(d) }) throw java.io.IOException("could not store the dictionary")
    }

    /**
     * addDictLocal stores a dictionary and announces the new list to the devices that see it: all
     * for a common one, the pair's device for its own. Only over a live session: a new one starts
     * with our hello, which carries the list.
     */
    private fun addDictLocal(d: Dict): Boolean {
        try {
            dicts.add(d)
        } catch (e: Exception) {
            return false
        }
        for (p in trust.list()) {
            if (!d.visible(p.addr) || current[p.addr] == null) continue
            peerState(p.addr)?.st?.push(Cbor.encode(listOf(3, 0, 5, dicts.ids(p.addr))), false, 0)
        }
        wake()
        return true
    }

    /** dictIds lists the dictionary ids peer sees, by slot (an empty id — a free slot). */
    fun dictIds(peer: String): List<ByteArray> = onLoop { dicts.ids(peer) }

    /**
     * dictState tells what compresses messages to peer: own — our newest dictionary trained for it
     * (null — none yet), using — the one used now (null — none, plain zstd), training — a new one
     * is due or being trained, or messages wait to be learnt.
     */
    fun dictState(peer: String): DictState = onLoop {
        val ps = peers[peer]
        DictState(
            own = dicts.own(peer)?.id,
            using = ps?.let { dicts.pick(peer, it.dicts, it.lacks).first?.id },
            training = learner.training(peer),
        )
    }

    /** historySize is how much of what we sent and got the history of peer holds (tests). */
    internal fun historySize(peer: String): Pair<Int, Int> = onLoop { learner.historySize(peer) }

    internal fun learnTick() = onLoop { learner.tick() }

    /** dropSession forgets the session with peer and makes what is in flight due at once (tests). */
    internal fun dropSession(peer: String) = onLoop {
        current.remove(peer)
        peers[peer]?.st?.inflight?.values?.forEach { it.sent = 0 }
        wake()
    }
}

private fun idsOf(v: Any?): List<ByteArray> = v.asList()?.mapNotNull { it as? ByteArray } ?: emptyList()

/** outOfOrder: results and errors of our calls, hellos and dictionary lists may skip the queue. */
/**
 * bootstrap: a hello or a dictionary list ([3, 0, 2|5, …]) carries what the receiver needs to
 * decompress, so it always goes uncompressed — a device that restarted must be able to read it.
 */
/** Held messages (spec §5): ask again after holdAsk, drop after holdDrop; tests shorten them. */
@Volatile internal var holdAsk = 15 * SEC
@Volatile internal var holdDrop = 120 * SEC

internal fun bootstrap(m: ByteArray): Boolean =
    m.size > 3 && m[1].toInt() == 3 && m[2].toInt() == 0 && (m[3].toInt() == 2 || m[3].toInt() == 5)

private fun outOfOrder(m: ByteArray): Boolean {
    if (m.size < 2) return false
    return m[1].toInt() == 1 || m[1].toInt() == 2 || (m.size > 3 && m[1].toInt() == 3 && m[2].toInt() == 0 && (m[3].toInt() == 2 || m[3].toInt() == 5))
}
