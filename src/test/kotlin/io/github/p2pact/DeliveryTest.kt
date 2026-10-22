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
}
