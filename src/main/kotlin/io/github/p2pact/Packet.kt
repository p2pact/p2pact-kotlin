package io.github.p2pact

// Packets (spec §4.1): type (1) | receiver's session index (4) | packet number (4) | ciphertext | tag (16).

internal const val P_INIT = 1 // known-device handshake start: [1][sender index 4][Noise]
internal const val P_RESP = 2 // reply: [2][receiver index 4][sender index 4][Noise]
internal const val P_PAIR1 = 3 // pairing: [3][invite id 16][sender index 4][Noise]
internal const val P_PAIR2 = 4 // [4][receiver index 4][sender index 4][Noise]
internal const val P_PAIR3 = 5 // [5][receiver index 4][Noise]
internal const val P_DATA = 6 // [6][receiver index 4][packet number 4][ciphertext + tag 16]
internal const val P_RESET = 7 // [7][index we do not know 4]

/** DATA_HEADER is the cleartext part of a data packet; it is also the associated data the tag covers. */
internal const val DATA_HEADER = 9

/** MAX_PACKET is the largest packet: it crosses any network without fragmentation. */
const val MAX_PACKET = 1200

internal fun u32(b: ByteArray, at: Int): Long =
    ((b[at].toLong() and 0xff) shl 24) or ((b[at + 1].toLong() and 0xff) shl 16) or
        ((b[at + 2].toLong() and 0xff) shl 8) or (b[at + 3].toLong() and 0xff)

internal fun be32(v: Long): ByteArray =
    byteArrayOf((v ushr 24).toByte(), (v ushr 16).toByte(), (v ushr 8).toByte(), v.toByte())

internal fun be64(v: Long): ByteArray = ByteArray(8) { (v ushr (56 - 8 * it)).toByte() }

internal fun u64(b: ByteArray, at: Int): Long {
    var n = 0L
    for (i in 0 until 8) n = (n shl 8) or (b[at + i].toLong() and 0xff)
    return n
}

internal fun cat(vararg parts: ByteArray): ByteArray {
    val out = ByteArray(parts.sumOf { it.size })
    var at = 0
    for (p in parts) {
        System.arraycopy(p, 0, out, at, p.size)
        at += p.size
    }
    return out
}

/**
 * Clock is monotonic nanoseconds. It starts an hour in, so 0 means "never" and is older than
 * anything (as Go's zero time).
 */
internal object Clock {
    private val start = System.nanoTime()
    fun nanos(): Long = System.nanoTime() - start + 3_600_000_000_000L
}

internal const val MS = 1_000_000L
internal const val SEC = 1_000_000_000L

/**
 * Window remembers the last 2048 packet numbers: replays and too-old packets are dropped
 * (spec §4.1).
 */
internal class Window {
    private var top = 0L // highest accepted number + 1 (0 — nothing yet)
    private val bits = LongArray(SIZE / 64)

    fun accept(n: Long): Boolean {
        when {
            n + 1 > top -> {
                val shift = n + 1 - top
                if (shift >= SIZE) bits.fill(0) else for (i in 0 until shift) clear((top + i) % SIZE)
                top = n + 1
            }
            top - n > SIZE -> return false
            has(n % SIZE) -> return false
        }
        set(n % SIZE)
        return true
    }

    private fun has(i: Long) = bits[(i / 64).toInt()] and (1L shl (i % 64).toInt()) != 0L
    private fun set(i: Long) { bits[(i / 64).toInt()] = bits[(i / 64).toInt()] or (1L shl (i % 64).toInt()) }
    private fun clear(i: Long) { bits[(i / 64).toInt()] = bits[(i / 64).toInt()] and (1L shl (i % 64).toInt()).inv() }

    companion object {
        const val SIZE = 2048
    }
}

/** Varints (LEB128, as Go's binary.Uvarint). */
internal class Bytes(initial: Int = 64) {
    var buf = ByteArray(initial)
    var size = 0

    fun byte(b: Int) {
        if (size == buf.size) buf = buf.copyOf(buf.size * 2)
        buf[size++] = b.toByte()
    }

    fun uvarint(v: Long) {
        var x = v
        while (x and 0x7fL.inv() != 0L) {
            byte(((x and 0x7f) or 0x80).toInt())
            x = x ushr 7
        }
        byte(x.toInt())
    }

    fun bytes(b: ByteArray, off: Int = 0, len: Int = b.size - off) {
        if (size + len > buf.size) buf = buf.copyOf(maxOf(buf.size * 2, size + len))
        System.arraycopy(b, off, buf, size, len)
        size += len
    }

    fun toByteArray(): ByteArray = buf.copyOf(size)
}

internal fun uvarintSize(v: Long): Int {
    var n = 1
    var x = v
    while (x and 0x7fL.inv() != 0L) { x = x ushr 7; n++ }
    return n
}

/** A reader of varints over a byte array; [ok] turns false on a short or overlong one. */
internal class VarReader(val b: ByteArray, var pos: Int) {
    var ok = true

    fun uvarint(): Long {
        var x = 0L
        var s = 0
        while (true) {
            if (pos >= b.size || s > 63) { ok = false; return 0 }
            val c = b[pos++].toInt() and 0xff
            if (c < 0x80) return x or (c.toLong() shl s)
            x = x or ((c and 0x7f).toLong() shl s)
            s += 7
        }
    }
}
