package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * A node paused for a computer's sleep sends nothing; on resume it speaks first. With
 * P2PACT_RELAY="<host:port> <certificate fingerprint>" the devices also share a relay, and its
 * connection is checked to close.
 */
class PauseTest {
    @get:Rule
    val tmp = TemporaryFolder()
    private val nodes = ArrayList<Node>()
    private val roads = ArrayList<AttachedRoad>()

    @After
    fun closeAll() {
        roads.forEach { it.detach() }
        nodes.forEach { it.close() }
        keepaliveGap = 25 * SEC
    }

    private val relay: RelayHint? = System.getenv("P2PACT_RELAY")?.trim()?.takeIf { it.isNotEmpty() }?.let {
        val (addr, fp) = it.split(Regex("\\s+"))
        RelayHint(addr, fp)
    }

    private fun node(name: String, start: Boolean = true): Node {
        val n = Node(tmp.newFolder(), name)
        n.listenUdp("127.0.0.1:0")
        n.listenTcp("127.0.0.1:0")
        relay?.let { n.setRelay(it) }
        n.handle(64, 1) { _, args -> listOf(args[0]) }
        if (start) n.start()
        nodes.add(n)
        return n
    }

    private fun pair(owner: Node, joiner: Node) {
        joiner.join(Invite.parse(owner.invite(60_000).toString()), 10_000)
        until("the owner knows the joiner") { owner.trust.get(joiner.id.addr) != null }
    }

    private fun until(what: String, cond: () -> Boolean) {
        for (i in 0 until 500) {
            if (cond()) return
            Thread.sleep(10)
        }
        fail(what)
    }

    private fun sent(n: Node, ble: FakeBle) = n.udpRoad!!.stats().sentPackets + n.tcpRoad!!.stats().sentPackets + ble.sent.get()

    @Test
    fun pausedIsSilentAndResumesAtOnce() {
        keepaliveGap = 100 * MS
        val a = node("laptop")
        val b = node("phone")
        val events = LinkedBlockingQueue<String>()
        b.onEvent = { _, _, _, args -> events.add(args[0] as String) }
        pair(a, b)
        // a Bluetooth LE link both ways, bound to each other
        val ab = FakeBle(BLE_MTU, BLE_RATE, 1)
        val ba = FakeBle(BLE_MTU, BLE_RATE, 1)
        val cfg = AppRoadConfig("ble", stream = true, mtu = BLE_MTU, rate = BLE_RATE)
        val ra = a.attachRoad(b.id.addr, ab, cfg).also { roads.add(it) }
        ba.deliver(ra)
        val rb = b.attachRoad(a.id.addr, ba, cfg).also { roads.add(it) }
        ab.deliver(rb)
        assertArrayEquals("before".toByteArray(), b.call(a.id.addr, 64, 1, "before".toByteArray(), timeoutMs = 5000).get()[0] as ByteArray)
        a.tcpRoad!!.send(b.tcpRoad!!.localAddr, ByteArray(5)) // a holds a TCP connection
        until("a TCP connection") { a.tcpRoad!!.connections > 0 }
        if (relay != null) until("on the pair's relay channel") { a.relayChannels > 0 }

        a.setPaused(true)
        a.setPaused(true) // twice: nothing more
        assertEquals(0, a.relayChannels)
        assertEquals(0, a.tcpRoad!!.connections)
        Socket().use { s -> // the listener refuses: the connection is closed at once
            val (h, p) = hostPort(a.tcpRoad!!.localAddr)!!
            s.connect(InetSocketAddress(h, p), 2000)
            s.soTimeout = 2000
            assertEquals(-1, try { s.getInputStream().read() } catch (e: java.io.IOException) { -1 })
        }
        val before = sent(a, ab)
        a.event(b.id.addr, 64, 1, "queued while paused")
        try {
            b.call(a.id.addr, 64, 1, "anyone?".toByteArray(), timeoutMs = 800).get()
            fail("a paused node answered a call")
        } catch (e: ExecutionException) {
            assertTrue(e.cause is TimeoutException)
        }
        Thread.sleep(10 * keepaliveGap / MS)
        assertEquals("packets from a paused node", 0L, sent(a, ab) - before)
        assertNull("an event left a paused node", events.poll())

        val start = System.nanoTime()
        a.setPaused(false)
        a.setPaused(false)
        assertEquals("queued while paused", events.poll(3, TimeUnit.SECONDS))
        assertArrayEquals("after".toByteArray(), b.call(a.id.addr, 64, 1, "after".toByteArray(), timeoutMs = 2000).get()[0] as ByteArray)
        assertTrue("back after ${(System.nanoTime() - start) / MS} ms", System.nanoTime() - start < 2 * SEC)
        if (relay != null) until("back on the relay") { a.relayChannels > 0 }
    }

    /** A call queued during a long sleep, after the other device has restarted: it goes once resumed. */
    @Test
    fun queuedCallAfterTheOtherRestarted() {
        val a = node("laptop")
        val b = node("phone")
        pair(a, b)
        assertArrayEquals("x".toByteArray(), a.call(b.id.addr, 64, 1, "x".toByteArray(), timeoutMs = 5000).get()[0] as ByteArray)
        a.setPaused(true)
        val call = a.call(b.id.addr, 64, 1, "queued".toByteArray(), timeoutMs = 10_000)
        Thread.sleep(300)
        val start = System.nanoTime()
        a.setPaused(false)
        assertArrayEquals("queued".toByteArray(), call.get()[0] as ByteArray)
        assertTrue(System.nanoTime() - start < 3 * SEC)
    }

    /** Pause and resume from many threads with close in the middle: no exception, nothing reopens after close. */
    @Test
    fun pauseRacesClose() {
        for (i in 0 until 5) {
            val a = node("laptop")
            val b = node("phone")
            pair(a, b)
            val errors = LinkedBlockingQueue<Throwable>()
            val threads = (0 until 4).map { g ->
                Thread {
                    try {
                        for (k in 0 until 50) {
                            a.setPaused((k + g) % 2 == 0)
                            try { a.event(b.id.addr, 64, 1, "x") } catch (e: Exception) { } // closed meanwhile
                        }
                    } catch (e: Throwable) {
                        errors.add(e)
                    }
                }.also { it.start() }
            }
            Thread.sleep(i * 5L)
            a.close()
            threads.forEach { it.join(10_000) }
            a.setPaused(false)
            assertNull(errors.poll())
            b.close()
        }
    }
}
