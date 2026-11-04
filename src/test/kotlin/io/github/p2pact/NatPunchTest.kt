package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketTimeoutException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** FakeObserver is a relay's UDP side: it answers observe requests with the address it is told. */
internal class FakeObserver : AutoCloseable {
    val socket = DatagramSocket(0, InetAddress.getByName("127.0.0.1"))
    val requests = CopyOnWriteArrayList<ByteArray>()
    @Volatile var reply: (DatagramPacket, ByteArray) -> ByteArray? = { _, nonce ->
        encodeObserveReply(nonce, InetAddress.getByName("203.0.113.5"), 4000)
    }
    val addr get() = "127.0.0.1:${socket.localPort}"

    init {
        Thread({
            val buf = ByteArray(100)
            while (!socket.isClosed) {
                val dp = DatagramPacket(buf, buf.size)
                try { socket.receive(dp) } catch (e: Exception) { break }
                val b = buf.copyOf(dp.length)
                requests.add(b)
                if (b.size != OBSERVE_REQUEST || b[0].toInt() != P_OBSERVE) continue
                val r = reply(dp, b.copyOfRange(1, 17)) ?: continue
                socket.send(DatagramPacket(r, r.size, dp.socketAddress))
            }
        }, "fake-observer").apply { isDaemon = true }.start()
    }

    override fun close() = socket.close()
}

class NatPunchTest : NetTestBase() {
    private val fakes = ArrayList<AutoCloseable>()

    @After
    fun reset() {
        observeEvery = 25 * SEC
        reflexiveStale = 80 * SEC
        punchGap = 200 * MS
        punchFor = 3 * SEC
        fakes.forEach { it.close() }
    }

    private fun fake() = FakeObserver().also { fakes.add(it) }

    @Test
    fun observeEncodesAndDecodes() {
        val nonce = ByteArray(16) { (it + 1).toByte() }
        val req = encodeObserve(nonce)
        assertEquals(36, req.size)
        assertEquals(8, req[0].toInt())
        assertArrayEquals(nonce, req.copyOfRange(1, 17))
        assertTrue(req.drop(17).all { it.toInt() == 0 })

        val rep = encodeObserveReply(nonce, InetAddress.getByName("203.0.113.5"), 51234)
        assertEquals(35, rep.size)
        assertEquals(8, rep[0].toInt())
        // IPv4-mapped IPv6, port big-endian
        assertArrayEquals(ByteArray(10) + byteArrayOf(-1, -1, 203.toByte(), 0, 113, 5), rep.copyOfRange(17, 33))
        assertArrayEquals(byteArrayOf((51234 shr 8).toByte(), 51234.toByte()), rep.copyOfRange(33, 35))
        val o = decodeObserveReply(rep)!!
        assertArrayEquals(nonce, o.nonce)
        assertEquals("203.0.113.5", o.ip.hostAddress)
        assertEquals(51234, o.port)

        val v6 = decodeObserveReply(encodeObserveReply(nonce, InetAddress.getByName("2001:db8::7"), 9))!!
        assertEquals("2001:db8:0:0:0:0:0:7", v6.ip.hostAddress)

        assertNull(decodeObserveReply(rep.copyOf(34)))
        assertNull(decodeObserveReply(rep.copyOf(36)))
        assertNull(decodeObserveReply(rep.also { it[0] = 6 }))
    }

    @Test
    fun reflexiveAddressJoinsTheHintsAndIsReplaced() {
        observeEvery = 300 * MS
        val relay = fake()
        val a = node("a")
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("reflexive learnt") { a.reflexiveAddr == "203.0.113.5:4000" }
        val local = a.udpRoad!!.localAddr
        val h = a.announcedHints()
        assertEquals(listOf(local, "203.0.113.5:4000"), h.udp)
        assertEquals(36, relay.requests.first().size)

        // the address changes: the old one is replaced, not kept
        relay.reply = { _, n -> encodeObserveReply(n, InetAddress.getByName("203.0.113.5"), 4001) }
        eventually("reflexive replaced") { a.reflexiveAddr == "203.0.113.5:4001" }
        assertEquals(listOf(local, "203.0.113.5:4001"), a.announcedHints().udp)
    }

    /** Advertised hints get the reflexive address too (as in Go and Swift): it is where our socket is reached from outside. */
    @Test
    fun reflexiveAddressJoinsAdvertisedHints() {
        observeEvery = 300 * MS
        val relay = fake()
        val a = node("a")
        a.setAdvertise(Hints(udp = listOf("198.51.100.1:5000")))
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("reflexive learnt") { a.reflexiveAddr == "203.0.113.5:4000" }
        val h = a.announcedHints()
        assertEquals(listOf("198.51.100.1:5000", "203.0.113.5:4000"), h.udp)
        assertEquals(relay.addr, h.relay?.addr)

        // advertising no UDP addresses at all (relay only): the reflexive one is still announced
        a.setAdvertise(Hints())
        assertEquals(listOf("203.0.113.5:4000"), a.announcedHints().udp)
    }

    /** A relay that stops answering: its last answer goes stale and the reflexive address leaves the hints. */
    @Test
    fun unansweredReflexiveAddressIsDropped() {
        observeEvery = 300 * MS
        reflexiveStale = 1500 * MS
        val relay = fake()
        val a = node("a")
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("reflexive learnt") { a.reflexiveAddr == "203.0.113.5:4000" }
        relay.reply = { _, _ -> null }
        eventually("reflexive dropped") { a.reflexiveAddr == null }
        assertEquals(listOf(a.udpRoad!!.localAddr), a.announcedHints().udp)
        // the relay answers again: the address comes back
        relay.reply = { _, n -> encodeObserveReply(n, InetAddress.getByName("203.0.113.5"), 4000) }
        eventually("reflexive learnt again") { a.reflexiveAddr == "203.0.113.5:4000" }
    }

    /** Forced observes (relay set, socket changed, resumed) go out at most once a second. */
    @Test
    fun forcedObservesAreLimitedToOneASecond() {
        val relay = fake()
        val a = node("a")
        repeat(5) { a.setRelay(RelayHint(relay.addr, "")) }
        eventually("asked") { relay.requests.isNotEmpty() }
        Thread.sleep(300)
        assertEquals(1, relay.requests.size)
        // the pending forced one goes after the second is up, once
        eventually("asked again after a second") { relay.requests.size == 2 }
        Thread.sleep(1500)
        assertEquals(2, relay.requests.size)
    }

    @Test
    fun reflexiveAlreadyLocalIsNotDuplicated() {
        observeEvery = 300 * MS
        val relay = fake()
        val a = node("a")
        val local = a.udpRoad!!.localAddr
        relay.reply = { dp, n -> encodeObserveReply(n, dp.address, dp.port) }
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("reflexive learnt") { a.reflexiveAddr == local }
        assertEquals(listOf(local), a.announcedHints().udp)
    }

    @Test
    fun repliesWithAWrongNonceOrFromAnotherAddressAreIgnored() {
        observeEvery = 300 * MS
        val relay = fake()
        val stranger = DatagramSocket(0, InetAddress.getByName("127.0.0.1")).also { fakes.add(it) }
        relay.reply = { dp, n ->
            // wrong nonce from the relay; right nonce from another socket
            val bad = n.copyOf().also { it[0] = (it[0] + 1).toByte() }
            val b = encodeObserveReply(bad, InetAddress.getByName("203.0.113.9"), 1)
            socketSend(relay, b, dp)
            val g = encodeObserveReply(n, InetAddress.getByName("203.0.113.9"), 2)
            stranger.send(DatagramPacket(g, g.size, dp.socketAddress))
            null
        }
        val a = node("a")
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("asked at least twice") { relay.requests.size >= 2 }
        Thread.sleep(300)
        assertNull(a.reflexiveAddr)
    }

    private fun socketSend(relay: FakeObserver, b: ByteArray, to: DatagramPacket) =
        relay.socket.send(DatagramPacket(b, b.size, to.socketAddress))

    @Test
    fun noRelayNoObserve() {
        observeEvery = 100 * MS
        val a = node("a")
        Thread.sleep(1500)
        assertNull(a.reflexiveAddr)
    }

    /** burstPackets counts what reaches a socket standing in for the device's reflexive address for the given time. */
    private fun burstPackets(sink: DatagramSocket, ms: Long): Int {
        val n = AtomicInteger()
        sink.soTimeout = 100
        val end = System.currentTimeMillis() + ms
        val buf = ByteArray(2048)
        while (System.currentTimeMillis() < end) {
            try { sink.receive(DatagramPacket(buf, buf.size)); n.incrementAndGet() } catch (e: SocketTimeoutException) { }
        }
        return n.get()
    }

    @Test
    fun learningOurAddressBurstsToPeersNotOnADirectRoad() {
        punchFor = 1500 * MS
        val a = node("a")
        val b = node("b")
        pair(a, b)
        echo(b, a, byteArrayOf(1))
        val sink = DatagramSocket(0, InetAddress.getByName("127.0.0.1")).also { fakes.add(it) }
        a.trust.setHints(b.id.addr, "", "", Hints(udp = listOf("127.0.0.1:${sink.localPort}")))
        a.forceIndirect = true
        val before = a.punchBursts
        val relay = fake()
        observeEvery = 300 * MS
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("burst started") { a.punchBursts > before }
        val got = burstPackets(sink, 1800)
        // every 200 ms for 1.5 s: about 8, and it stops
        assertTrue("packets in the burst: $got", got in 5..12)
        assertEquals(0, burstPackets(sink, 600))
    }

    @Test
    fun noBurstWhileTheSessionIsOnADirectRoad() {
        val a = node("a")
        val b = node("b")
        pair(a, b)
        echo(b, a, byteArrayOf(1))
        echo(a, b, byteArrayOf(1))
        val relay = fake()
        observeEvery = 300 * MS
        a.setRelay(RelayHint(relay.addr, ""))
        eventually("reflexive learnt") { a.reflexiveAddr != null }
        // b hears the new hints and changes its own too
        eventually("b learns our reflexive address") { b.trust.get(a.id.addr)!!.hints.udp.contains("203.0.113.5:4000") }
        b.rebindUdp("127.0.0.1:0")
        eventually("a hears b's new address") { a.trust.get(b.id.addr)!!.hints.udp.contains(b.udpRoad!!.localAddr) }
        assertEquals(0, a.punchBursts)
        assertEquals(0, b.punchBursts)
    }

    @Test
    fun aHelloWithNewUdpHintsBurstsWhenNotDirect() {
        val a = node("a")
        val b = node("b")
        pair(a, b)
        echo(b, a, byteArrayOf(1))
        a.forceIndirect = true
        b.rebindUdp("127.0.0.1:0") // b's addresses change: its hello says so
        eventually("a hears b's new address") { a.trust.get(b.id.addr)!!.hints.udp.contains(b.udpRoad!!.localAddr) }
        assertTrue(a.punchBursts >= 1)
    }

    @Test
    fun pausedNodeDoesNotObserveOrBurst() {
        observeEvery = 200 * MS
        val relay = fake()
        val a = node("a")
        a.setPaused(true)
        a.setRelay(RelayHint(relay.addr, ""))
        Thread.sleep(2500)
        assertEquals(0, relay.requests.size)
        assertNull(a.reflexiveAddr)
        a.setPaused(false)
        eventually("asked after resume") { relay.requests.isNotEmpty() }
        eventually("reflexive learnt") { a.reflexiveAddr != null }
        assertNotNull(a.reflexiveAddr)
        assertFalse(a.punchBursts < 0)
    }
}
