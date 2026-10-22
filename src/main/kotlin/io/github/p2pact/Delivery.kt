package io.github.p2pact

// Delivery (spec §5). The decrypted content of a data packet:
//
//   flags (1) | [ack] | [varint part number | [varint stream | varint number in stream] | part]
//   ack = varint "all up to N" | varint count | count × (varint missing, varint received)
//
// A message is cut into packet-sized parts numbered consecutively. Messages on stream 0 are
// delivered in the order sent; a message on another stream is delivered as soon as it is
// complete and its stream's earlier messages are delivered. Anything unacknowledged is
// retransmitted while the session lives.

internal const val F_ACK = 1 // carries an ack
internal const val F_MSG = 2 // carries a message part
internal const val F_MORE = 4 // more parts follow
internal const val F_ZIP = 8 // compressed; set on every part of a compressed message
internal const val F_ONCE = 16 // do not ack (keepalive)
internal const val F_FIRST = 32 // the first part of a message on a stream: stream and its number follow
internal const val F_CONT = 64 // a further part of a message on a stream

/** ACK_BUDGET is the room for an ack in a packet; ranges beyond it are left out. */
internal const val ACK_BUDGET = 60

/** MAX_PART: 1200 − 9 (header) − 16 (tag) − 1 (flags) − the ack − up to 5 (part number) − up to 20 (stream). */
internal const val MAX_PART = MAX_PACKET - DATA_HEADER - 16 - 1 - ACK_BUDGET - 5 - 20

internal const val RTO_START = 500 * MS // before the first round-trip sample
internal const val RTO_MIN = 50 * MS
internal const val RTO_MAX = 4 * SEC
internal const val ACK_DELAY = 40 * MS
internal const val CWND_START = 32.0 // parts in flight at first
internal const val CWND_MIN = 32.0
internal const val CWND_MAX = 4096.0
internal const val GOT_MAX = 8192 // parts held above the first gap
internal const val MSG_MAX = 16 shl 20 // longer messages abort reassembly

internal class Part(
    var seq: Long,
    val data: ByteArray,
    val more: Boolean,
    val zip: Boolean,
    val stream: Long, // 0 — the ordered default stream
    val reply: Boolean = false, // part of a result or error: it answers a call of this boot only
) {
    var num = 0L // message number in its stream (from 1), on the first part
    var first = false // first part of a message on a stream
    var copies = ArrayList<Long>(1) // when each copy was sent
    var timer = false // the last copy went because the timer ran out
    var sent = 0L
    var tries = 0
    var delivered = 0L // bytes delivered when this part was sent: its ack gives a rate sample
}

internal class RPart(
    val data: ByteArray,
    val more: Boolean,
    val zip: Boolean,
    val first: Boolean,
    val streamed: Boolean,
    val stream: Long,
    val num: Long,
) {
    var used = false
}

/** Delivery is delivery to one device, both directions. */
internal class Delivery {
    var next = 1L // number of our next part
    var inflight = HashMap<Long, Part>()
    var queue = ArrayDeque<Part>()

    // congestion control: the window grows while acks arrive and halves on a loss while a queue
    // builds; the retransmission timer follows the measured round trip (RFC 6298)
    var cwnd = CWND_START
    var ssthresh = CWND_MAX
    var srtt = 0L
    var rttvar = 0L
    var minRTT = 0L
    var lastCut = 0L

    // delivery rate, as in BBR: bytes acknowledged per second, the maximum of recent samples
    var delivered = 0L
    var inflightBytes = 0L
    var reliable = false // the current road is a reliable stream: no window of our own
    var bw = 0.0
    var bwAt = 0L

    var snext = HashMap<Long, Long>() // last message number sent per stream

    var recvAck = 1L // the first part not received yet
    var got = HashMap<Long, RPart>() // parts received from recvAck on, and earlier ones not yet used
    var next0 = 1L // the next part to take for stream 0
    private var asm0 = Bytes(0)
    private var zip0 = false
    private var streams = HashMap<Long, RStream>()
    var ackDue = 0L // when to ack (0 — nothing to ack)
    var unacked = 0 // parts received since our last ack

    private class RStream {
        var next = 1L
        val ready = HashMap<Long, Pair<ByteArray, Boolean>>()
    }

    private fun resetRecv() {
        snext = HashMap()
        recvAck = 1; next0 = 1; got = HashMap(); asm0 = Bytes(0); zip0 = false
        streams = HashMap()
        ackDue = 0; unacked = 0
    }

    /** rto is the retransmission timeout for a part sent for the first time. */
    fun rto(): Long = if (srtt == 0L) RTO_START else (srtt + 4 * rttvar).coerceIn(RTO_MIN, RTO_MAX)

    /** rate takes a delivery-rate sample: the estimate is the maximum of the last ~2 seconds. */
    private fun rate(now: Long, r: Double) {
        if (r >= bw || now - bwAt > 2 * SEC) {
            bw = r
            bwAt = now
        }
    }

    /**
     * canSend tells whether one more part may go: the congestion window, capped by twice the
     * bandwidth-delay product while a queue is building.
     */
    fun canSend(): Boolean {
        if (reliable) return inflight.size < CWND_MAX // the road controls congestion itself
        if (inflight.size >= cwnd.toInt()) return false
        if (queueing() && bw > 0 && minRTT > 0) {
            return inflightBytes < (2 * bw * minRTT / SEC).toLong() + 4 * MAX_PART
        }
        return true
    }

    /**
     * timeout is when a part is due for retransmission: the current estimate plus the time the
     * link needs for the parts sent ahead of it, doubled on retries only on signs of congestion
     * or after three tries in a row.
     */
    fun timeout(p: Part, ahead: Long): Long {
        var t = rto()
        if (bw > 0) t += (ahead * MAX_PART / bw * SEC).toLong()
        val backoff = if (queueing()) p.tries - 1 else maxOf(0, p.tries - 3)
        var i = 0
        while (i < backoff && t < RTO_MAX) {
            t *= 2
            i++
        }
        return minOf(t, RTO_MAX)
    }

    /** sample takes a round-trip measurement (Karn's rule); a grown round trip leaves slow start. */
    private fun sample(rtt: Long) {
        if (minRTT == 0L || rtt < minRTT) minRTT = rtt
        if (cwnd < ssthresh && rtt > minRTT + maxOf(minRTT / 2, 4 * MS)) ssthresh = cwnd
        if (srtt == 0L) {
            srtt = rtt
            rttvar = rtt / 2
            return
        }
        val d = Math.abs(srtt - rtt)
        rttvar = (3 * rttvar + d) / 4
        srtt = (7 * srtt + rtt) / 8
    }

    /** acked grows the window for n newly acknowledged parts. */
    private fun acked(n: Int) {
        val queue = queueing()
        repeat(n) {
            if (cwnd < ssthresh || !queue) cwnd++ else cwnd += 1 / cwnd
        }
        cwnd = minOf(cwnd, CWND_MAX)
    }

    /** queueing reports that the round trip has grown well above its minimum. */
    fun queueing(): Boolean = srtt > minRTT + maxOf(minRTT / 2, 10 * MS)

    /** lost halves the window, at most once per round trip, and only while a queue builds. */
    private fun lost(now: Long) {
        if (now - lastCut < maxOf(srtt, RTO_MIN) || !queueing()) return
        lastCut = now
        ssthresh = maxOf(cwnd / 2, CWND_MIN)
        cwnd = ssthresh
    }

    /**
     * reset handles a restart of the other device (spec §4.2): our unacknowledged parts are
     * renumbered from 1; receiving restarts from 1 too. Replies are dropped: the calls they
     * answer ended with the restart, and the new call ids start from 1 again, so a stale reply
     * would answer a new call.
     */
    fun reset() {
        val pending = inflight.values.sortedBy { it.seq }.toMutableList()
        pending.addAll(queue)
        next = 1; inflight = HashMap(); queue = ArrayDeque(); inflightBytes = 0
        resetRecv()
        // the path may be another one now: measure it afresh (a stale minimum round trip would
        // read every round trip as a queue, or every ack of a retransmission as a real one)
        cwnd = CWND_START; ssthresh = CWND_MAX; lastCut = 0
        srtt = 0; rttvar = 0; minRTT = 0; bw = 0.0; bwAt = 0
        for (p in pending) {
            if (p.reply) continue
            p.seq = next++
            p.copies = ArrayList(1); p.sent = 0; p.tries = 0; p.timer = false
            if (p.first) { // the other side expects each stream from message 1 again
                val n = (snext[p.stream] ?: 0) + 1
                snext[p.stream] = n
                p.num = n
            }
            queue.addLast(p)
        }
    }

    /**
     * push queues a message in parts on a stream (0 — the ordered default); zip marks it
     * compressed, reply — a result or error (dropped if the other device restarts).
     */
    fun push(msg: ByteArray, zip: Boolean, stream: Long, reply: Boolean = false) {
        var num = 0L
        if (stream != 0L) {
            num = (snext[stream] ?: 0) + 1
            snext[stream] = num
        }
        var at = 0
        var first = true
        while (true) {
            val n = minOf(msg.size - at, MAX_PART)
            val p = Part(next++, msg.copyOfRange(at, at + n), at + n < msg.size, zip, stream, reply)
            if (stream != 0L && first) {
                p.first = true
                p.num = num
            }
            first = false
            queue.addLast(p)
            at += n
            if (at >= msg.size) return
        }
    }

    class Next(val plain: ByteArray, val retry: Boolean)

    /**
     * nextPacket builds the next packet content: an overdue retransmission, a new part, or just
     * an ack; null — nothing to send. forceAck asks for an ack now.
     */
    fun nextPacket(now: Long, forceAck: Boolean): Next? {
        var p: Part? = null
        var lowest = 0L
        for (seq in inflight.keys) if (lowest == 0L || seq < lowest) lowest = seq
        for (x in inflight.values) {
            if (now - x.sent >= timeout(x, x.seq - lowest) && (p == null || x.seq < p.seq)) p = x
        }
        var retry = false
        if (p != null) {
            retry = true
            p.timer = p.sent != 0L // not taken as lost by an ack: the timer ran out
            lost(now)
        } else if (queue.isNotEmpty() && canSend()) {
            p = queue.removeFirst()
            inflight[p.seq] = p
            inflightBytes += p.data.size
        }
        val received = recvAck > 1 || got.isNotEmpty()
        val ack = received && (p != null || forceAck || (ackDue != 0L && now >= ackDue))
        if (p == null && !ack) return null
        var flags = 0
        val b = Bytes(if (p != null) p.data.size + 96 else 64)
        b.byte(0)
        if (ack) {
            flags = flags or F_ACK
            appendAck(b)
            ackDue = 0
            unacked = 0
        }
        if (p != null) {
            flags = flags or F_MSG
            if (p.more) flags = flags or F_MORE
            if (p.zip) flags = flags or F_ZIP
            b.uvarint(p.seq)
            if (p.first) {
                flags = flags or F_FIRST
                b.uvarint(p.stream)
                b.uvarint(p.num)
            } else if (p.stream != 0L) {
                flags = flags or F_CONT
            }
            b.bytes(p.data)
            p.copies.add(now)
            p.delivered = delivered
            p.sent = now
            p.tries++
        }
        b.buf[0] = flags.toByte()
        return Next(b.toByteArray(), retry)
    }

    /**
     * appendAck writes the ack: all up to recvAck−1, then the ranges received above the first
     * gap as (missing, received) counts, as many as fit in ACK_BUDGET.
     */
    private fun appendAck(b: Bytes) {
        b.uvarint(recvAck - 1)
        val above = got.keys.filter { it > recvAck }.sorted()
        val ranges = Bytes(ACK_BUDGET)
        var count = 0
        var cursor = recvAck
        var i = 0
        while (i < above.size) {
            var j = i
            while (j + 1 < above.size && above[j + 1] == above[j] + 1) j++
            val gap = above[i] - cursor
            val run = (j - i + 1).toLong()
            if (ranges.size + uvarintSize(gap) + uvarintSize(run) > ACK_BUDGET - 12) break
            ranges.uvarint(gap)
            ranges.uvarint(run)
            count++
            cursor = above[j] + 1
            i = j + 1
        }
        b.uvarint(count.toLong())
        b.bytes(ranges.buf, 0, ranges.size)
    }

    /**
     * receive handles packet content from the other device; deliver is called for every message
     * as it becomes deliverable (zip — compressed; stream — the stream it came on). False — the
     * content is malformed.
     */
    fun receive(now: Long, b: ByteArray, deliver: (ByteArray, Boolean, Long) -> Unit): Boolean {
        if (b.isEmpty()) return false
        val flags = b[0].toInt() and 0xff
        val r = VarReader(b, 1)
        if (flags and F_ACK != 0) {
            val upto = r.uvarint()
            val count = r.uvarint()
            if (!r.ok || count > 64) return false
            var done = 0
            var newest = 0L // the latest send time among the parts this ack covers
            fun ack(seq: Long) {
                val p = inflight[seq] ?: return
                // an ack sooner than the minimum round trip after a timer's copy is for an earlier
                // copy: the timer fired on a queue, not a loss (RFC 8985 §6.2). The newest copy's
                // time would declare every part sent between the copies lost, and their copies
                // would fire the same way, again and again. The ack is not ambiguous, so it is a
                // round-trip sample (Karn's rule skips it otherwise): without one the timer
                // would stay shorter than the queue for good.
                var sent = p.sent
                if (p.timer && now - p.sent < minRTT) {
                    for ((i, c) in p.copies.withIndex()) if (i == 0 || now - c >= minRTT) sent = c
                    sample(now - sent)
                }
                if (sent > newest) newest = sent
                delivered += p.data.size
                if (p.tries == 1) {
                    sample(now - p.sent)
                    val dt = now - p.sent
                    if (dt > 0) rate(now, (delivered - p.delivered).toDouble() / dt * SEC)
                }
                inflight.remove(seq)
                inflightBytes -= p.data.size
                done++
            }
            for (seq in inflight.keys.filter { it <= upto }) ack(seq)
            var beyond = 0L
            var cursor = upto + 1
            for (i in 0 until count) {
                val gap = r.uvarint()
                val run = r.uvarint()
                if (!r.ok || run > GOT_MAX || gap > Int.MAX_VALUE) return false
                val start = cursor + gap
                if (inflight.isNotEmpty()) for (seq in start until start + run) ack(seq)
                beyond += run
                cursor = start + run
            }
            acked(done)
            // time-based loss detection (RACK, RFC 8985): a part sent a quarter of a round trip
            // before one that arrived, and still not acknowledged, is lost — resend it now. A
            // reliable road loses nothing: there a late ack only means the road's own queue.
            if (newest != 0L && !reliable) {
                val reorder = maxOf(srtt / 4, MS)
                var anyLost = false
                for (p in inflight.values) {
                    if (p.sent != 0L && p.sent + reorder < newest) {
                        p.sent = 0
                        anyLost = true
                    }
                }
                if (anyLost) lost(now)
            }
            // fast retransmit: three parts after the gap arrived, so the gap is lost
            if (beyond > 0 && !reliable) {
                val p = inflight[upto + 1]
                if (p != null && (beyond >= 3 || now - p.sent >= maxOf(srtt / 4, ACK_DELAY))) p.sent = 0
            }
        }
        if (flags and F_MSG == 0) return true
        val seq = r.uvarint()
        if (!r.ok) return false
        var first = false
        var streamed = false
        var st = 0L
        var num = 0L
        if (flags and F_FIRST != 0) {
            st = r.uvarint()
            num = r.uvarint()
            if (!r.ok) return false
            first = true
            streamed = true
        } else if (flags and F_CONT != 0) {
            streamed = true
        }
        if (ackDue == 0L) ackDue = now + ACK_DELAY
        // like TCP: ack at once every second part, and on anything out of order
        unacked++
        if (unacked >= 2 || seq != recvAck) ackDue = now
        if (seq < recvAck || got.containsKey(seq) || got.size >= GOT_MAX) return true // duplicate, or too far ahead
        got[seq] = RPart(b.copyOfRange(r.pos, b.size), flags and F_MORE != 0, flags and F_ZIP != 0, first, streamed, st, num)
        while (got.containsKey(recvAck)) recvAck++
        if (streamed) assemble(seq, deliver)
        take0(deliver)
        val it = got.entries.iterator()
        while (it.hasNext()) { // forget parts that are used and acknowledged
            val (k, x) = it.next()
            if (x.used && k < recvAck && k < next0) it.remove()
        }
        return true
    }

    /** assemble completes the stream message part seq belongs to, if all its parts are here. */
    private fun assemble(seq: Long, deliver: (ByteArray, Boolean, Long) -> Unit) {
        var start = seq
        while (true) {
            val x = got[start] ?: return
            if (x.used) return
            if (x.first) break
            if (!x.streamed || start == 0L) return
            start--
        }
        val head = got[start]!!
        val msg = Bytes(0)
        var end = start
        while (true) {
            val x = got[end]
            if (x == null || !x.streamed || x.used || (end != start && x.first)) return // a part is still missing
            if (msg.size + x.data.size > MSG_MAX) return
            msg.bytes(x.data)
            if (!x.more) break
            end++
        }
        for (k in start..end) got[k]!!.used = true
        val rs = streams.getOrPut(head.stream) { RStream() }
        if (head.num < rs.next) return // a duplicate of a delivered message
        rs.ready[head.num] = msg.toByteArray() to head.zip
        while (true) {
            val m = rs.ready.remove(rs.next) ?: return
            rs.next++
            deliver(m.first, m.second, head.stream)
        }
    }

    /** take0 delivers stream 0 messages in order; parts of stream messages are skipped. */
    private fun take0(deliver: (ByteArray, Boolean, Long) -> Unit) {
        while (true) {
            val x = got[next0] ?: return
            next0++
            if (x.streamed) continue
            x.used = true
            if (asm0.size == 0) zip0 = x.zip
            if (asm0.size + x.data.size > MSG_MAX) {
                asm0 = Bytes(0)
                continue
            }
            asm0.bytes(x.data)
            if (x.more) continue
            val msg = asm0.toByteArray()
            asm0 = Bytes(0)
            deliver(msg, zip0, 0)
        }
    }

    /** idle reports that nothing waits to be sent or acknowledged. */
    fun idle(): Boolean = inflight.isEmpty() && queue.isEmpty()

    /** stuck: a part was tried four times (the session looks dead). */
    fun stuck(): Boolean = inflight.values.any { it.tries >= 4 }
}

/** keepalive is a liveness packet content: flags only, not acknowledged. */
internal fun keepalive(): ByteArray = byteArrayOf(F_ONCE.toByte())
