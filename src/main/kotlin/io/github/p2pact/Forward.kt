package io.github.p2pact

// Via a third device (spec §3): a device both trust carries a pair's packets inside its own
// sessions with each of them — event 0.7 forward [to, packet] to it, event 0.8 forwarded
// [from, packet] from it. The packets stay end-to-end encrypted: the forwarder sees what a relay
// sees. It forwards only for pairs its person opted it in for (Node.allowForward), one hop only,
// within a rate limit, and never ahead of its own traffic. The node's side is in Node.kt.

internal const val EV_FORWARD = 7 // event 0.7 forward [to, packet]: to a forwarder
internal const val EV_FORWARDED = 8 // event 0.8 forwarded [from, packet]: from a forwarder

/** DEFAULT_FORWARD_RATE: bytes a second a forwarder carries in one direction of a pair, unless [Node.forwardRate] says otherwise. */
const val DEFAULT_FORWARD_RATE = 256 shl 10

internal const val FORWARD_BURST = 64 shl 10 // bytes a direction may send at once above its rate, and may wait

/**
 * FORWARD_QUEUE: past this many parts waiting to go to a device, packets to forward to it (and
 * our own packets for a forwarder) are dropped — our own traffic goes first.
 */
internal const val FORWARD_QUEUE = 128

private const val B32 = "abcdefghijklmnopqrstuvwxyz234567"

/** addrBytes is an address as it goes on the wire: 16 bytes; null — not an address. */
internal fun addrBytes(addr: String): ByteArray? {
    if (addr.length != 26) return null
    val out = java.io.ByteArrayOutputStream(16)
    var v = 0
    var bits = 0
    for (c in addr) {
        val d = B32.indexOf(c)
        if (d < 0) return null
        v = (v shl 5) or d
        bits += 5
        if (bits >= 8) {
            out.write((v ushr (bits - 8)) and 0xff)
            bits -= 8
            v = v and ((1 shl bits) - 1)
        }
    }
    return out.toByteArray().takeIf { it.size == 16 }
}

/** addrText is an address from the wire. */
internal fun addrText(b: ByteArray): String {
    val sb = StringBuilder()
    var bits = 0
    var v = 0
    for (x in b) {
        v = (v shl 8) or (x.toInt() and 0xff)
        bits += 8
        while (bits >= 5) {
            sb.append(B32[(v ushr (bits - 5)) and 31])
            bits -= 5
        }
        v = v and ((1 shl bits) - 1)
    }
    if (bits > 0) sb.append(B32[(v shl (5 - bits)) and 31])
    return sb.toString()
}

/** forwardStream: forward events about a device go on the stream of its address's first 8 bytes. */
internal fun forwardStream(addr: ByteArray): Long {
    var s = 0L
    for (i in 0 until 8) s = (s shl 8) or (addr[i].toLong() and 0xff)
    return if (s == 0L) 1L else s
}

/** forwardMsg: events 0.7 and 0.8 — never compressed, run out of order. */
internal fun forwardMsg(m: ByteArray): Boolean =
    m.size > 3 && m[1].toInt() == 3 && m[2].toInt() == 0 && (m[3].toInt() == EV_FORWARD || m[3].toInt() == EV_FORWARDED)

internal class FwdPending(val to: String, val msg: ByteArray, val stream: Long, val size: Int)

/**
 * FwdBucket shapes one forwarded direction: a token bucket of the forward rate with bursts of
 * [FORWARD_BURST] bytes. What comes over the rate waits, up to a burst, and goes as the bucket
 * refills — the queue it builds tells the pair's congestion control to slow down; past that it is
 * dropped.
 */
internal class FwdBucket(var tokens: Double, var at: Long) {
    val q = ArrayDeque<FwdPending>()
    var queued = 0

    fun refill(now: Long, rate: Double) {
        if (now <= at) return
        tokens = minOf(FORWARD_BURST.toDouble(), tokens + rate * (now - at) / SEC)
        at = now
    }
}
