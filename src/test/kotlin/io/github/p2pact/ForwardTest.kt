package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.util.Collections
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.random.Random

/** Via a third device (spec §3): A and B reach each other only through X. */
class ForwardTest : NetTestBase() {
    private fun friends(p: Node, q: Node) {
        p.befriend(Peer(q.id.addr, q.id.key.public, q.name, ByteArray(0), Hints()))
        q.befriend(Peer(p.id.addr, p.id.key.public, p.name, ByteArray(0), Hints()))
    }

    /** A and B trust each other but know no address of the other and announce none; both are paired with X over UDP. */
    private val xDir by lazy { tmp.newFolder() }

    private fun three(): Triple<Node, Node, Node> {
        val a = node("A")
        val x = node("X", dir = xDir)
        val b = node("B")
        a.setAdvertise(Hints())
        b.setAdvertise(Hints())
        pair(x, a)
        pair(x, b)
        friends(a, b)
        return Triple(a, x, b)
    }

    private fun tap(x: Node): MutableList<ByteArray> {
        val seen = Collections.synchronizedList(ArrayList<ByteArray>())
        x.forwardTap = { _, _, pkt -> seen.add(pkt.copyOf()) }
        return seen
    }

    private fun ByteArray.contains(b: ByteArray): Boolean =
        (0..size - b.size).any { i -> b.indices.all { this[i + it] == b[it] } }

    private fun unreachable(from: Node, to: Node) {
        try {
            from.call(to.id.addr, 64, 1, "hi".toByteArray(), timeoutMs = 2000).get()
            fail("${from.name} reached ${to.name}")
        } catch (e: ExecutionException) {
        }
    }

    private fun forge(a: Node, x: Node, b: Node) =
        a.event(x.id.addr, 0, EV_FORWARD, addrBytes(b.id.addr), byteArrayOf(1) + Random.nextBytes(200))

    @Test
    fun talkThroughAThirdDevice() {
        val (a, x, b) = three()
        val seen = tap(x)
        x.allowForward(a.id.addr, b.id.addr, true)
        assertEquals(listOf(b.id.addr), x.forwards(a.id.addr))
        eventually("A hears X forwards to B") { a.forwarders(b.id.addr) == listOf(x.id.addr) }

        val marker = "plaintext-marker-that-X-must-never-see".toByteArray()
        val msg = ByteArray(100) { '.'.code.toByte() } + marker
        echo(a, b, msg)
        echo(b, a, msg)
        assertEquals(listOf("via:${x.id.addr}"), a.paths(b.id.addr))
        assertEquals(listOf("via:${x.id.addr}"), b.paths(a.id.addr))
        val long = Random.nextBytes(40 shl 10)
        echo(a, b, long)
        echo(b, a, long)

        val events = LinkedBlockingQueue<String>()
        a.onEvent = { _, _, _, args -> events.add(args[0] as String) }
        b.onEvent = { _, _, _, args -> events.add(args[0] as String) }
        a.event(b.id.addr, 64, 3, "to B")
        b.event(a.id.addr, 64, 3, "to A")
        val got = setOf(events.poll(5, TimeUnit.SECONDS), events.poll(5, TimeUnit.SECONDS))
        assertEquals(setOf("to A", "to B"), got)

        assertTrue("X forwarded nothing", seen.isNotEmpty())
        assertFalse("X saw the plaintext", synchronized(seen) { seen.any { it.contains(marker) } })
        assertTrue(x.forwardStats().first > 0)
        x.trust.flush()
        assertEquals(listOf(b.id.addr), Trust(xDir).get(a.id.addr)!!.forward) // kept with the trusted list
    }

    @Test
    fun refused() {
        val (a, x, b) = three()
        val seen = tap(x)
        unreachable(a, b) // nobody forwards
        forge(a, x, b)
        eventually("X drops the forged packet") { x.forwardStats().second > 0 }
        assertTrue(seen.isEmpty())

        val stranger = node("S", udp = false)
        try { x.allowForward(a.id.addr, stranger.id.addr, true); fail("forward to a stranger") } catch (e: IllegalArgumentException) { }
        try { x.allowForward(a.id.addr, x.id.addr, true); fail("forward to itself") } catch (e: IllegalArgumentException) { }

        x.allowForward(a.id.addr, b.id.addr, true)
        echo(a, b, "through X".toByteArray())
        x.forget(b.id.addr)
        eventually("A hears X no longer forwards to B") { a.forwarders(b.id.addr).isEmpty() }
        val before = seen.size
        val dropped = x.forwardStats().second
        forge(a, x, b)
        eventually("X drops a packet for a device it forgot") { x.forwardStats().second > dropped }
        assertEquals(before, seen.size)
    }

    @Test
    fun withdrawn() {
        val (a, x, b) = three()
        x.allowForward(a.id.addr, b.id.addr, true)
        echo(a, b, "through X".toByteArray())
        x.allowForward(b.id.addr, a.id.addr, false)
        eventually("both hear X no longer forwards") { a.forwarders(b.id.addr).isEmpty() && b.forwarders(a.id.addr).isEmpty() }
    }

    /** One hop only: X reaches B only through Y, so it does not forward A's packets to B. */
    @Test
    fun oneHop() {
        val a = node("A")
        val x = node("X")
        val y = node("Y")
        val b = node("B")
        a.setAdvertise(Hints())
        b.setAdvertise(Hints())
        pair(y, x)
        pair(y, b)
        pair(x, a)
        x.setAdvertise(Hints()) // Y and A know where X is; B never learns it
        friends(x, b)
        friends(a, b)
        y.allowForward(x.id.addr, b.id.addr, true)
        echo(x, b, "X to B through Y".toByteArray())
        x.allowForward(a.id.addr, b.id.addr, true)
        eventually("A hears X forwards to B") { a.forwarders(b.id.addr).size == 1 }
        unreachable(a, b)
        val (fw, dropped) = x.forwardStats()
        assertEquals(0L, fw)
        assertTrue(dropped > 0)
    }

    /** The rate limit: a burst, then the rate; over it packets wait up to a burst, then are dropped. */
    @Test
    fun bucket() {
        val a = node("A", udp = false)
        val b = node("B", udp = false)
        friends(a, b)
        val n = node("X", udp = false)
        n.befriend(Peer(b.id.addr, b.id.key.public, "B", ByteArray(0), Hints()))
        n.forwardRate = 1000
        val now = Clock.nanos()
        var offered = 0
        while (n.fwdOfferForTest("a>b", b.id.addr, 1000, now)) offered++
        val burst = FORWARD_BURST / 1000
        assertEquals(2 * burst, offered)
        assertEquals(burst.toLong(), n.forwardStats().first)
        n.fwdReleaseForTest(now + 400 * MS) // 536 left + 400: not enough
        assertEquals(burst.toLong(), n.forwardStats().first)
        n.fwdReleaseForTest(now + 2100 * MS)
        assertEquals(burst.toLong() + 2, n.forwardStats().first)
    }

    @Test
    fun addresses() {
        val a = node("A", udp = false)
        assertEquals(a.id.addr, addrText(addrBytes(a.id.addr)!!))
        assertEquals(null, addrBytes("nonsense"))
        assertEquals(16, addrBytes(a.id.addr)!!.size)
    }
}
