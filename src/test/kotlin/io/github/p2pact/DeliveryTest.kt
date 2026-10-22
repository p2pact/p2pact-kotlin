package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.PriorityQueue
import kotlin.random.Random

class DeliveryTest {
    private class InFlight(val at: Long, val to: Int, val plain: ByteArray)

    /** run moves messages between two delivery states over an emulated link with loss and jitter. */
    private fun run(loss: Double, reliable: Boolean, msgs: List<Pair<Long, ByteArray>>, seed: Int = 1): List<Pair<Long, ByteArray>> {
        val rnd = Random(seed)
        val a = Delivery()
        val b = Delivery()
        a.reliable = reliable
        b.reliable = reliable
        for ((st, m) in msgs) a.push(m, false, st)
        val got = ArrayList<Pair<Long, ByteArray>>()
        val wire = PriorityQueue<InFlight>(compareBy { it.at })
        var now = SEC
        val end = now + 120 * SEC
        while (now < end && got.size < msgs.size) {
            for ((i, d) in listOf(a, b).withIndex()) {
                while (true) {
                    val p = d.nextPacket(now, false) ?: break
                    if (rnd.nextDouble() >= loss) wire.add(InFlight(now + 10 * MS + rnd.nextLong(5 * MS), 1 - i, p.plain))
                }
            }
            while (wire.isNotEmpty() && wire.peek().at <= now) {
                val f = wire.poll()
                val ok = if (f.to == 1) b.receive(now, f.plain) { m, _, st -> got.add(st to m) } else a.receive(now, f.plain) { _, _, _ -> }
                assertTrue(ok)
            }
            now += MS
        }
        return got
    }

    private fun message(i: Int, size: Int) = ByteArray(size) { (i * 31 + it).toByte() }

    @Test
    fun inOrderWithoutLoss() {
        val msgs = (1..50).map { 0L to message(it, 100 + it * 97) }
        val got = run(0.0, false, msgs)
        assertEquals(msgs.size, got.size)
        for (i in msgs.indices) assertArrayEquals(msgs[i].second, got[i].second)
    }

    @Test
    fun lossAndLongMessages() {
        val msgs = (1..30).map { (it % 3).toLong() to message(it, if (it % 5 == 0) 40_000 else 300) }
        val got = run(0.2, false, msgs)
        assertEquals(msgs.size, got.size)
        for (st in 0L..2L) { // each stream in the order sent
            val want = msgs.filter { it.first == st }.map { it.second.hex() }
            assertEquals(want, got.filter { it.first == st }.map { it.second.hex() })
        }
    }

    @Test
    fun reliableRoad() {
        val msgs = (1..20).map { 1L to message(it, 5000) }
        assertEquals(msgs.size, run(0.0, true, msgs).size)
    }

    @Test
    fun ackRangesFitTheBudget() {
        val d = Delivery()
        // every other part arrives: many ranges above the first gap
        for (seq in 2L..400L step 2) {
            val b = Bytes()
            b.byte(F_MSG)
            b.uvarint(seq)
            b.bytes(byteArrayOf(1))
            d.receive(SEC, b.toByteArray()) { _, _, _ -> }
        }
        val p = d.nextPacket(SEC, true)!!.plain
        assertTrue(p.size <= 1 + ACK_BUDGET)
        val r = VarReader(p, 1)
        assertEquals(0L, r.uvarint()) // nothing in order yet
        assertTrue(r.uvarint() > 5)
    }

    @Test
    fun resetRenumbersFromOne() {
        val a = Delivery()
        a.push(ByteArray(10), false, 2)
        a.nextPacket(SEC, false)
        a.push(ByteArray(10), false, 2)
        a.reset()
        assertEquals(2, a.queue.size)
        assertEquals(1L, a.queue[0].seq)
        assertEquals(1L, a.queue[0].num)
        assertEquals(2L, a.queue[1].num)
    }

    @Test
    fun malformedIsRefused() {
        val d = Delivery()
        assertEquals(false, d.receive(SEC, ByteArray(0)) { _, _, _ -> })
        assertEquals(false, d.receive(SEC, byteArrayOf(F_ACK.toByte(), 0x80.toByte())) { _, _, _ -> })
    }

    /**
     * The other device restarted: our unacknowledged calls go again, renumbered; replies to its
     * old calls are dropped (they would answer its new calls with the same ids), and the path is
     * measured afresh.
     */
    @Test
    fun resetDropsReplies() {
        val a = Delivery()
        a.push("call".toByteArray(), false, 1)
        a.push("reply".toByteArray(), false, 2, reply = true)
        a.push("queued reply".toByteArray(), false, 2, reply = true)
        a.nextPacket(SEC, false)
        a.nextPacket(SEC, false)
        a.srtt = 80 * MS; a.minRTT = 60 * MS; a.bw = 1e6; a.cwnd = 100.0
        a.reset()
        assertEquals(1, a.queue.size)
        assertEquals("call", String(a.queue[0].data))
        assertEquals(1L, a.queue[0].seq)
        assertEquals(1L, a.queue[0].num)
        assertEquals(2L, a.next)
        assertEquals(0L, a.srtt)
        assertEquals(0L, a.minRTT)
        assertEquals(0.0, a.bw, 0.0)
        assertEquals(CWND_START, a.cwnd, 0.0)
    }

    /**
     * A timeout fired by a queue, not a loss: the ack of the first copy comes just after the
     * retransmission. It must not mark the parts sent between the copies lost, and it is a
     * round-trip sample, so the timer catches up with the queue (spec §5).
     */
    @Test
    fun ackOfAnEarlierCopyAfterATimeout() {
        val a = Delivery()
        val b = Delivery()
        val t0 = SEC
        fun exchange(at: Long, vararg pkts: ByteArray) {
            for (p in pkts) b.receive(at, p) { _, _, _ -> }
            val ack = b.nextPacket(at, true)!!.plain
            a.receive(at + MS, ack) { _, _, _ -> }
        }
        a.push("first".toByteArray(), false, 1) // a round-trip sample: 60 ms
        exchange(t0 + 59 * MS, a.nextPacket(t0, false)!!.plain)
        assertEquals(60 * MS, a.minRTT)
        val sent = t0 + 100 * MS
        val pkts = ArrayList<ByteArray>()
        for (i in 0 until 5) {
            a.push("queued".toByteArray(), false, 1)
            pkts.add(a.nextPacket(sent, false)!!.plain)
        }
        val at = sent + a.timeout(a.inflight[2]!!, 0)
        assertTrue("no retransmission on timeout", a.nextPacket(at, false)!!.retry)
        val srtt = a.srtt
        exchange(at + MS, pkts[0]) // the first copy, late behind a queue
        assertEquals(null, a.inflight[2L])
        for ((seq, p) in a.inflight) assertTrue("part $seq taken as lost", p.sent != 0L)
        assertTrue("no round-trip sample from the ack", a.srtt > srtt)
    }
}
