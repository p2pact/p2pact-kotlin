package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

/**
 * FakeBle is one direction of an in-memory Bluetooth LE link: writes of at most mtu bytes, rate
 * bytes a second (a write waits for the link, as a GATT write waits for its acknowledgement), a
 * one-way delay; it keeps order and loses nothing, as GATT does while connected.
 */
internal class FakeBle(val mtu: Int, val rate: Int, val delayMs: Long) : AppRoad {
    /** instead of the delay: a local TCP connection (a pipe to another program) and its other end */
    var conn: Socket? = null
    var peer: Socket? = null
    /** a datagram is lost (a datagram road only) */
    var loss: (() -> Boolean)? = null
    private val q = LinkedBlockingQueue<Pair<ByteArray, Long>>()
    private var free = 0L // when the link is free again (only send's thread touches it)
    val sent = AtomicLong()
    val tooBig = AtomicBoolean()

    override fun send(b: ByteArray) {
        if (b.size > mtu) {
            tooBig.set(true)
            throw IOException("longer than the MTU")
        }
        val now = System.nanoTime()
        if (free < now - 10 * MS) free = now // idle: the link starts now (an oversleep does not slow it)
        free += b.size * SEC / rate
        val wait = free - System.nanoTime()
        if (wait > 2 * MS) Thread.sleep(wait / MS, (wait % MS).toInt())
        sent.addAndGet(b.size.toLong())
        if (loss?.invoke() == true) return
        val c = conn
        if (c != null) {
            c.getOutputStream().write(b)
            return
        }
        q.put(b.copyOf() to free + delayMs * MS)
    }

    /** deliver hands what was written to the road at the other end, in order, after the delay. */
    fun deliver(to: AttachedRoad) {
        val p = peer
        if (p != null) { // the other end of the connection reads, at most an MTU at a time
            Thread({
                val buf = ByteArray(mtu)
                try {
                    while (true) {
                        val k = p.getInputStream().read(buf)
                        if (k < 0) break
                        to.receive(buf.copyOf(k))
                    }
                } catch (e: IOException) {
                }
            }, "fake-ble").apply { isDaemon = true }.start()
            return
        }
        Thread({
            while (true) {
                val (b, due) = try { q.take() } catch (e: InterruptedException) { return@Thread }
                val wait = due - System.nanoTime()
                if (wait > 2 * MS) Thread.sleep(wait / MS, (wait % MS).toInt()) // sleeping per write would slow the link
                to.receive(b)
            }
        }, "fake-ble").apply { isDaemon = true }.start()
    }
}

internal const val BLE_MTU = 20 // the smallest GATT write
internal const val BLE_RATE = 40 shl 10 // a Bluetooth LE link under load

class AppRoadTest {
    @get:Rule
    val tmp = TemporaryFolder()
    private val nodes = ArrayList<Node>()
    private val roads = ArrayList<AttachedRoad>()
    private val sockets = ArrayList<Socket>()

    @After
    fun closeAll() {
        roads.forEach { it.detach() }
        sockets.forEach { it.close() }
        nodes.forEach { it.close() }
        probeEvery = 30 * SEC
    }

    private fun node(name: String, udp: Boolean, dir: File = tmp.newFolder(), learn: Boolean = true): Node {
        val n = Node(dir, name)
        n.noLearning = !learn
        if (udp) n.listenUdp("127.0.0.1:0")
        n.handle(64, 1) { _, args -> listOf(args[0]) }
        n.start()
        nodes.add(n)
        return n
    }

    private fun pair(owner: Node, joiner: Node) {
        val p = joiner.join(Invite.parse(owner.invite(60_000).toString()), 10_000)
        assertEquals(owner.id.addr, p.addr)
        for (i in 0 until 200) {
            if (owner.trust.get(joiner.id.addr) != null) return
            Thread.sleep(10)
        }
        fail("the owner does not know the joiner")
    }

    private fun echo(from: Node, to: Node, data: ByteArray, timeoutMs: Long = 15_000) {
        val res = from.call(to.id.addr, 64, 1, data, timeoutMs = timeoutMs).get()
        assertArrayEquals(data, res[0] as ByteArray)
    }

    /**
     * bleLink attaches a Bluetooth LE link between a and b with a one-way delay, or over local TCP
     * connections when delayMs < 0: bound to each other when bind is set, else learnt from what arrives.
     */
    private fun bleLink(a: Node, b: Node, bind: Boolean, delayMs: Long = 10): Pair<FakeBle, FakeBle> {
        val ab = FakeBle(BLE_MTU, BLE_RATE, delayMs)
        val ba = FakeBle(BLE_MTU, BLE_RATE, delayMs)
        if (delayMs < 0) {
            for (f in listOf(ab, ba)) {
                ServerSocket(0, 1, InetAddress.getLoopbackAddress()).use { ss ->
                    f.conn = Socket(ss.inetAddress, ss.localPort).also { it.tcpNoDelay = true }
                    f.peer = ss.accept()
                }
                sockets.add(f.conn!!)
                sockets.add(f.peer!!)
            }
        }
        val cfg = AppRoadConfig("ble", stream = true, mtu = BLE_MTU, rate = BLE_RATE)
        // each side's deliverer starts before the other side may send
        val ra = a.attachRoad(if (bind) b.id.addr else null, ab, cfg)
        ba.deliver(ra)
        val rb = b.attachRoad(if (bind) a.id.addr else null, ba, cfg)
        ab.deliver(rb)
        roads.add(ra)
        roads.add(rb)
        return ab to ba
    }

    /**
     * Two devices with nothing but a Bluetooth LE link (20-byte writes, 40 KB/s): they pair over
     * it (the road is bound by the handshake), call both ways, and carry long messages without
     * resending them — with the latency of a radio link, and over a local connection with none (a
     * pipe to another program: a small packet's round trip is then no measure of the link at all).
     */
    @Test
    fun bluetoothOnly() = bluetooth(10, listOf(13, 10000, 30 shl 10, 13, 1800, 200 shl 10, 13))

    @Test
    fun bluetoothOverAPipe() = bluetooth(-1, listOf(13, 10000, 30 shl 10, 13, 1800, 64 shl 10, 13))

    private fun bluetooth(delayMs: Long, sizes: List<Int>) {
        // no dictionary of their own: its download would take the link from the messages timed here
        val a = node("Mac", false, learn = false)
        val b = node("Phone", false, learn = false)
        val (ab, ba) = bleLink(a, b, false, delayMs)
        pair(a, b)
        echo(b, a, "hello over bluetooth".toByteArray())
        assertEquals(listOf("app:ble"), a.paths(b.id.addr))
        for (size in sizes) {
            for ((from, to) in listOf(b to a, a to b)) {
                val before = ab.sent.get() + ba.sent.get()
                val start = System.nanoTime()
                echo(from, to, Random(size).nextBytes(size), 30_000)
                val took = System.nanoTime() - start
                val sent = ab.sent.get() + ba.sent.get() - before
                if (size > 1000) println("$size bytes there and back over 20-byte writes at 40 KB/s: ${took / MS} ms, $sent bytes on the link")
                // a short message right after a long one does not wait behind resent parts or a backed-off timer
                val ideal = 2L * size * SEC / BLE_RATE
                assertTrue("$size bytes there and back took ${took / MS} ms, the link needs ${ideal / MS} ms", took <= ideal * 5 / 4 + 300 * MS)
                // both ways, plus headers, acks and framing (about 4 %); resent parts would show here
                assertTrue("$sent bytes on the link for $size there and back: parts were resent", sent <= 2L * size * 110 / 100 + 1000)
            }
        }
        assertFalse("a write longer than the MTU", ab.tooBig.get() || ba.tooBig.get())
    }

    /** Known devices that have only a Bluetooth link the application names: attaching it starts a session over it. */
    @Test
    fun boundRoadStartsASession() {
        val da = tmp.newFolder()
        val db = tmp.newFolder()
        val a0 = node("Mac", true, da)
        val b0 = node("Phone", true, db)
        pair(a0, b0)
        a0.close()
        b0.close()
        nodes.clear()
        // the same devices again, now without UDP
        val a = node("Mac", false, da)
        val b = node("Phone", false, db)
        bleLink(a, b, true)
        echo(a, b, "first packet".toByteArray())
        echo(b, a, Random(2).nextBytes(5000))
        assertTrue(a.handshakes + b.handshakes >= 1)
    }

    /** A session moves from Bluetooth to UDP when UDP comes up, and back when it goes. */
    @Test
    fun switchesToUdpAndBack() {
        probeEvery = 300 * MS
        val a = node("Mac", true)
        val b = node("Phone", true)
        pair(a, b)
        echo(a, b, "over udp".toByteArray())

        a.udpRoad!!.drop = { true }
        b.udpRoad!!.drop = { true }
        val (ab, ba) = bleLink(a, b, true)
        fun carried(size: Int): Long {
            val before = ab.sent.get() + ba.sent.get()
            echo(a, b, Random(size).nextBytes(size))
            return ab.sent.get() + ba.sent.get() - before
        }
        carried(8000).let { assertTrue("UDP down: $it bytes over Bluetooth for 16000", it >= 16000) }

        a.udpRoad!!.drop = null
        b.udpRoad!!.drop = null
        var i = 0
        while (true) {
            Thread.sleep(200)
            if (carried(50_000) < 5000) break
            if (++i == 20) fail("the session stays on Bluetooth while UDP is up")
        }

        a.udpRoad!!.drop = { true }
        b.udpRoad!!.drop = { true }
        carried(8000).let { assertTrue("UDP down again: $it bytes over Bluetooth for 16000", it >= 16000) }
    }

    /** A datagram road the application provides (a packet each, some lost): pairing, calls and a long message, the losses found as on UDP. */
    @Test
    fun datagramRoad() {
        val a = node("Mac", false, learn = false)
        val b = node("Phone", false, learn = false)
        val rnd = Random(7)
        val loss = { synchronized(rnd) { rnd.nextDouble() < 0.02 } }
        val ab = FakeBle(MAX_ROAD_PACKET, 100 shl 10, 5).also { it.loss = loss }
        val ba = FakeBle(MAX_ROAD_PACKET, 100 shl 10, 5).also { it.loss = loss }
        val cfg = AppRoadConfig("wifi-aware", stream = false, mtu = MAX_ROAD_PACKET, rate = 100 shl 10)
        val ra = a.attachRoad(null, ab, cfg)
        ba.deliver(ra)
        val rb = b.attachRoad(null, ba, cfg)
        ab.deliver(rb)
        roads.add(ra)
        roads.add(rb)
        pair(a, b)
        echo(a, b, "over datagrams".toByteArray())
        assertEquals(listOf("app:wifi-aware"), b.paths(a.id.addr))
        val size = 100 shl 10
        val before = ab.sent.get() + ba.sent.get()
        val start = System.nanoTime()
        echo(b, a, Random(3).nextBytes(size), 30_000)
        val took = System.nanoTime() - start
        val sent = ab.sent.get() + ba.sent.get() - before
        println("100 KB there and back at 100 KB/s, 2 % lost: ${took / MS} ms, $sent bytes on the link")
        // headers about 4 %, the lost 2 % again; a lost part resent on every ack until its copy got
        // through the link's queue would show here
        assertTrue("$sent bytes on the link for ${2 * size}: parts were resent needlessly", sent <= 2L * size * 110 / 100)
        assertTrue("took ${took / MS} ms, the link needs 2 s", took <= 4 * SEC)
    }

    /** A datagram road must carry a whole packet; a stream road takes any MTU. */
    @Test
    fun config() {
        val a = node("Mac", false)
        try {
            a.attachRoad(null, FakeBle(512, BLE_RATE, 0), AppRoadConfig(stream = false, mtu = 512))
            fail("a datagram road of 512 bytes was taken")
        } catch (e: IllegalArgumentException) {
        }
        val r = a.attachRoad(null, FakeBle(512, BLE_RATE, 0), AppRoadConfig(stream = true, mtu = 512))
        r.detach()
        assertFalse("a detached road takes packets", r.send("", byteArrayOf(1)))
        try {
            a.attachRoad("nobody", FakeBle(512, BLE_RATE, 0), AppRoadConfig(stream = true, mtu = 512))
            fail("a road to an unknown device was taken")
        } catch (e: IllegalArgumentException) {
        }
    }
}
