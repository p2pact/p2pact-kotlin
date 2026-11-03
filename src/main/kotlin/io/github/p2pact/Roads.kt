package io.github.p2pact

import java.io.DataInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.Inet4Address
import java.net.Inet6Address
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.NetworkInterface
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Road carries packets (spec §3). It encrypts nothing and trusts nothing; it may lose,
 * duplicate and reorder packets. send never blocks for long: it queues or drops.
 */
interface Road {
    val name: String
    fun send(addr: String, pkt: ByteArray): Boolean
}

/** Endpoint is where to send on a road: a UDP or TCP address, or a relay channel. */
data class Endpoint(val road: Road, val addr: String) {
    val key: String get() = road.name + "|" + addr
}

internal typealias Receiver = (ByteArray, Endpoint) -> Unit

/** RoadStats counts payload bytes and packets. */
data class RoadStats(val sentBytes: Long, val sentPackets: Long, val recvBytes: Long, val recvPackets: Long)

/** hostPort splits "host:port" and "[v6]:port". */
internal fun hostPort(addr: String): Pair<String, Int>? {
    val i = addr.lastIndexOf(':')
    if (i <= 0) return null
    val port = addr.substring(i + 1).toIntOrNull() ?: return null
    var host = addr.substring(0, i)
    if (host.startsWith("[") && host.endsWith("]")) host = host.substring(1, host.length - 1)
    return host to port
}

internal fun joinHostPort(ip: InetAddress, port: Int): String =
    if (ip is Inet6Address) "[${ip.hostAddress?.substringBefore('%')}]:$port" else "${ip.hostAddress}:$port"

/** localAddrs lists our addresses for hints: the listen address, or every IPv4 network address when listening on all. */
internal fun localAddrs(ip: InetAddress, port: Int): List<String> {
    if (!ip.isAnyLocalAddress) return listOf(joinHostPort(ip, port))
    val out = ArrayList<String>()
    try {
        for (ni in NetworkInterface.getNetworkInterfaces()?.toList() ?: emptyList()) {
            if (!ni.isUp) continue
            for (a in ni.inetAddresses.toList()) {
                if (a is Inet4Address && !a.isLoopbackAddress && !a.isLinkLocalAddress) out.add(joinHostPort(a, port))
            }
        }
    } catch (e: SocketException) {
        // no interfaces readable: no hints
    }
    return out
}

/** UdpRoad is the main road: one packet is one datagram (spec §3). */
class UdpRoad internal constructor(bind: String, private val recv: Receiver) : Road {
    override val name = "udp"
    private val socket: DatagramSocket
    private val addrs = ConcurrentHashMap<String, InetSocketAddress>()
    private val sentBytes = AtomicLong()
    private val sentPackets = AtomicLong()
    private val recvBytes = AtomicLong()
    private val recvPackets = AtomicLong()

    /** drop is for tests: it says a packet is lost, both ways. */
    @Volatile
    var drop: (() -> Boolean)? = null

    init {
        val (host, port) = hostPort(bind) ?: ("" to (bind.removePrefix(":").toIntOrNull() ?: 0))
        val sa = if (host.isEmpty()) InetSocketAddress(port) else InetSocketAddress(InetAddress.getByName(host), port)
        socket = DatagramSocket(null as java.net.SocketAddress?)
        socket.reuseAddress = false
        // a burst of retransmissions must not overflow the receive queue
        socket.receiveBufferSize = 1 shl 20
        socket.sendBufferSize = 1 shl 20
        socket.bind(sa)
        Thread({ run() }, "p2pact-udp").apply { isDaemon = true }.start()
    }

    val localAddr: String get() = joinHostPort(socket.localAddress, socket.localPort)

    internal fun localAddrs(): List<String> = localAddrs(socket.localAddress, socket.localPort)

    fun stats() = RoadStats(sentBytes.get(), sentPackets.get(), recvBytes.get(), recvPackets.get())

    override fun send(addr: String, pkt: ByteArray): Boolean {
        sentBytes.addAndGet(pkt.size.toLong())
        sentPackets.incrementAndGet()
        if (drop?.invoke() == true) return true
        val to = addrs[addr] ?: run {
            val (h, p) = hostPort(addr) ?: return false
            val a = try { InetSocketAddress(InetAddress.getByName(h), p) } catch (e: IOException) { return false }
            addrs[addr] = a
            a
        }
        return try {
            socket.send(DatagramPacket(pkt, pkt.size, to))
            true
        } catch (e: IOException) {
            false
        }
    }

    private fun run() {
        val buf = ByteArray(2048)
        val dp = DatagramPacket(buf, buf.size)
        while (!socket.isClosed) {
            try {
                dp.setData(buf)
                socket.receive(dp)
            } catch (e: IOException) {
                if (socket.isClosed) return
                continue
            }
            recvBytes.addAndGet(dp.length.toLong())
            recvPackets.incrementAndGet()
            if (drop?.invoke() == true) continue
            recv(buf.copyOf(dp.length), Endpoint(this, joinHostPort(dp.address, dp.port)))
        }
    }

    internal fun close() = socket.close()
}

/** Link is one connection of a stream road: what is read (buffered), what is written, how to close it. */
internal class Link(val input: InputStream, val output: OutputStream, val close: () -> Unit)

/**
 * StreamRoad carries packets over connections that keep order, each framed as a 2-byte length and
 * the packet (spec §3): TCP (the fallback road where UDP is blocked) and the local socket. Connections
 * are made on demand and reused both ways; sessions do not depend on them.
 */
abstract class StreamRoad internal constructor(final override val name: String, private val recv: Receiver) : Road {
    private val conns = ConcurrentHashMap<String, Conn>() // by remote address as we know it
    internal val closed = AtomicBoolean()
    @Volatile internal var paused = false // no connections (the node pauses); the listener stays and refuses them
    private val sentBytes = AtomicLong()
    private val sentPackets = AtomicLong()
    private val recvBytes = AtomicLong()
    private val recvPackets = AtomicLong()

    private inner class Conn(val addr: String) {
        val out = LinkedBlockingQueue<ByteArray>(1024)
        @Volatile var link: Link? = null
        private val stopped = AtomicBoolean()

        fun stop() {
            if (!stopped.compareAndSet(false, true)) return
            try { link?.let { it.close() } } catch (e: IOException) { }
            out.offer(ByteArray(0)) // wakes the writer
            conns.remove(addr, this)
        }

        val isStopped get() = stopped.get()

        /** serve runs one connection: a writer thread and this reader. */
        fun serve(l: Link) {
            link = l
            if (stopped.get()) { l.close(); return }
            Thread({
                try {
                    val os = l.output
                    while (!stopped.get()) {
                        val f = out.poll(30, TimeUnit.SECONDS) ?: continue
                        if (f.isEmpty()) break
                        os.write(f)
                        // write what is queued too before flushing
                        while (true) {
                            val g = out.poll() ?: break
                            if (g.isEmpty()) { stopped.set(true); break }
                            os.write(g)
                        }
                        os.flush()
                    }
                } catch (e: Exception) {
                } finally {
                    stop()
                }
            }, "p2pact-$name-w").apply { isDaemon = true }.start()
            try {
                val inp = DataInputStream(l.input)
                val ep = Endpoint(this@StreamRoad, addr)
                while (!stopped.get()) {
                    val n = inp.readUnsignedShort()
                    if (n > MAX_PACKET + 64) break
                    val pkt = ByteArray(n)
                    inp.readFully(pkt)
                    recvBytes.addAndGet(2L + n)
                    recvPackets.incrementAndGet()
                    recv(pkt, ep)
                }
            } catch (e: Exception) {
            } finally {
                stop()
            }
        }
    }

    /** connect dials addr: null — it did not answer. It runs on a thread of its own. */
    internal abstract fun connect(addr: String): Link?

    /** accepted serves a connection a listener took in; addr names it for answers. */
    internal fun accepted(addr: String, link: Link) {
        val c = Conn(addr)
        conns.put(addr, c)?.stop()
        Thread({ c.serve(link) }, "p2pact-$name-r").apply { isDaemon = true }.start()
    }

    fun stats() = RoadStats(sentBytes.get(), sentPackets.get(), recvBytes.get(), recvPackets.get())

    /** send queues a packet to addr, dialing in the background if there is no connection. */
    override fun send(addr: String, pkt: ByteArray): Boolean {
        if (pkt.size > MAX_PACKET + 64 || closed.get() || paused) return false
        var c = conns[addr]
        if (c == null || c.isStopped) {
            val nc = Conn(addr)
            c = conns.putIfAbsent(addr, nc) ?: nc.also { dial(it) }
        }
        val frame = ByteArray(2 + pkt.size)
        frame[0] = (pkt.size shr 8).toByte()
        frame[1] = pkt.size.toByte()
        System.arraycopy(pkt, 0, frame, 2, pkt.size)
        if (!c.out.offer(frame)) return false // the queue is full
        sentBytes.addAndGet(frame.size.toLong())
        sentPackets.incrementAndGet()
        return true
    }

    private fun dial(c: Conn) {
        Thread({
            val l = try { connect(c.addr) } catch (e: Exception) { null }
            if (l == null) c.stop() else c.serve(l)
        }, "p2pact-$name-dial").apply { isDaemon = true }.start()
    }

    /** pause closes every connection and refuses new ones (true), or takes them again. */
    internal fun pause(on: Boolean) {
        paused = on
        if (on) for (c in conns.values) c.stop()
    }

    /** connections counts open connections (tests). */
    internal val connections: Int get() = conns.size

    internal open fun close() {
        closed.set(true)
        for (c in conns.values) c.stop()
    }
}

/**
 * TcpRoad is the fallback road where UDP is blocked (spec §3): a 2-byte length, then the
 * packet. Connections are made on demand and reused both ways; sessions do not depend on them.
 */
class TcpRoad internal constructor(bind: String, recv: Receiver) : StreamRoad("tcp", recv) {
    private val server: ServerSocket?

    init {
        server = if (bind.isEmpty()) null else {
            val (host, port) = hostPort(bind) ?: ("" to (bind.removePrefix(":").toIntOrNull() ?: 0))
            val ss = ServerSocket()
            ss.reuseAddress = true
            ss.bind(if (host.isEmpty()) InetSocketAddress(port) else InetSocketAddress(InetAddress.getByName(host), port))
            Thread({
                while (!closed.get()) {
                    val s = try { ss.accept() } catch (e: IOException) { break }
                    if (paused) {
                        try { s.close() } catch (e: IOException) { }
                        continue
                    }
                    accepted(joinHostPort(s.inetAddress, s.port), link(s))
                }
            }, "p2pact-tcp-accept").apply { isDaemon = true }.start()
            ss
        }
    }

    private fun link(s: Socket): Link {
        try { s.tcpNoDelay = true } catch (e: IOException) { }
        s.soTimeout = 3 * 60_000
        return Link(s.getInputStream().buffered(64 shl 10), s.getOutputStream()) { s.close() }
    }

    override fun connect(addr: String): Link? {
        val (h, p) = hostPort(addr) ?: return null
        val s = Socket()
        try {
            s.connect(InetSocketAddress(InetAddress.getByName(h), p), 10_000)
        } catch (e: IOException) {
            try { s.close() } catch (x: IOException) { }
            return null
        }
        return link(s)
    }

    /** localAddr is the listen address ("" — dial only). */
    val localAddr: String get() = server?.let { joinHostPort(it.inetAddress, it.localPort) } ?: ""

    internal fun localAddrs(): List<String> = server?.let { localAddrs(it.inetAddress, it.localPort) } ?: emptyList()

    override fun close() {
        closed.set(true)
        try { server?.close() } catch (e: IOException) { }
        super.close()
    }
}
