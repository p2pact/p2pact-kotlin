package io.github.p2pact

import java.io.ByteArrayOutputStream

/**
 * CBOR (RFC 8949), the subset messages use (spec §6): integers, byte and text strings, arrays,
 * maps, booleans, null and floats. Decoded values are Long, ByteArray, String, List<Any?>,
 * Map<Any?, Any?>, Boolean, Double or null; tags are skipped.
 */
object Cbor {
    class CborException(msg: String) : Exception(msg)

    fun encode(v: Any?): ByteArray {
        val out = ByteArrayOutputStream()
        write(out, v)
        return out.toByteArray()
    }

    private fun head(out: ByteArrayOutputStream, major: Int, n: Long) {
        val m = major shl 5
        when {
            n in 0..23 -> out.write(m or n.toInt())
            n in 24..0xff -> { out.write(m or 24); out.write(n.toInt()) }
            n in 0x100..0xffff -> { out.write(m or 25); out.write((n shr 8).toInt()); out.write(n.toInt() and 0xff) }
            n in 0x10000..0xffffffffL -> {
                out.write(m or 26)
                for (s in 24 downTo 0 step 8) out.write(((n shr s) and 0xff).toInt())
            }
            else -> { // up to 2^64 - 1, n read as unsigned
                out.write(m or 27)
                for (s in 56 downTo 0 step 8) out.write(((n ushr s) and 0xff).toInt())
            }
        }
    }

    private fun write(out: ByteArrayOutputStream, v: Any?) {
        when (v) {
            null -> out.write(0xf6)
            is Boolean -> out.write(if (v) 0xf5 else 0xf4)
            is Byte, is Short, is Int, is Long -> {
                val n = (v as Number).toLong()
                if (n >= 0) head(out, 0, n) else head(out, 1, -1 - n)
            }
            is UInt -> head(out, 0, v.toLong())
            is ULong -> head(out, 0, v.toLong())
            is Float, is Double -> {
                out.write(0xfb)
                val bits = (v as Number).toDouble().toRawBits()
                for (s in 56 downTo 0 step 8) out.write(((bits ushr s) and 0xff).toInt())
            }
            is ByteArray -> { head(out, 2, v.size.toLong()); out.write(v) }
            is String -> { val b = v.toByteArray(Charsets.UTF_8); head(out, 3, b.size.toLong()); out.write(b) }
            is List<*> -> { head(out, 4, v.size.toLong()); for (x in v) write(out, x) }
            is Array<*> -> { head(out, 4, v.size.toLong()); for (x in v) write(out, x) }
            is Map<*, *> -> {
                head(out, 5, v.size.toLong())
                for ((k, x) in v) { write(out, k); write(out, x) }
            }
            else -> throw CborException("cbor: cannot encode ${v::class.java.name}")
        }
    }

    /** decode reads one value; trailing bytes are an error. */
    fun decode(b: ByteArray): Any? {
        val r = Reader(b)
        val v = r.value(0)
        if (r.pos != b.size) throw CborException("cbor: trailing bytes")
        return v
    }

    private class Reader(val b: ByteArray) {
        var pos = 0

        fun byte(): Int {
            if (pos >= b.size) throw CborException("cbor: short")
            return b[pos++].toInt() and 0xff
        }

        fun arg(info: Int): Long = when {
            info < 24 -> info.toLong()
            info == 24 -> byte().toLong()
            info == 25 -> ((byte() shl 8) or byte()).toLong()
            info == 26 -> { var n = 0L; repeat(4) { n = (n shl 8) or byte().toLong() }; n }
            info == 27 -> { var n = 0L; repeat(8) { n = (n shl 8) or byte().toLong() }; n }
            else -> throw CborException("cbor: bad length")
        }

        fun bytes(n: Long): ByteArray {
            if (n < 0 || n > b.size - pos) throw CborException("cbor: short")
            val out = b.copyOfRange(pos, pos + n.toInt())
            pos += n.toInt()
            return out
        }

        fun count(n: Long): Int {
            if (n < 0 || n > b.size - pos) throw CborException("cbor: too long") // every item takes a byte
            return n.toInt()
        }

        fun value(depth: Int): Any? {
            if (depth > 64) throw CborException("cbor: too deep")
            val ib = byte()
            val major = ib shr 5
            val info = ib and 31
            if (info == 31 && major in 2..5) return indefinite(major, depth)
            return when (major) {
                0 -> arg(info)
                1 -> -1 - arg(info)
                2 -> bytes(arg(info))
                3 -> String(bytes(arg(info)), Charsets.UTF_8)
                4 -> { val n = count(arg(info)); ArrayList<Any?>(n).apply { repeat(n) { add(value(depth + 1)) } } }
                5 -> { val n = count(arg(info)); LinkedHashMap<Any?, Any?>().apply { repeat(n) { put(value(depth + 1), value(depth + 1)) } } }
                6 -> { arg(info); value(depth + 1) } // a tag: its content
                else -> when (info) {
                    20 -> false
                    21 -> true
                    22, 23 -> null
                    25 -> half(arg(25).toInt())
                    26 -> Float.fromBits(arg(26).toInt()).toDouble()
                    27 -> Double.fromBits(arg(27))
                    else -> if (info < 24) null else { arg(info); null }
                }
            }
        }

        fun indefinite(major: Int, depth: Int): Any? {
            val items = ArrayList<Any?>()
            while (true) {
                if (pos < b.size && (b[pos].toInt() and 0xff) == 0xff) { pos++; break }
                items.add(value(depth + 1))
            }
            return when (major) {
                2 -> items.fold(ByteArray(0)) { acc, x -> acc + (x as? ByteArray ?: throw CborException("cbor: bad chunk")) }
                3 -> items.joinToString("") { it as? String ?: throw CborException("cbor: bad chunk") }
                4 -> items
                else -> {
                    if (items.size % 2 != 0) throw CborException("cbor: odd map")
                    LinkedHashMap<Any?, Any?>().apply { for (i in items.indices step 2) put(items[i], items[i + 1]) }
                }
            }
        }

        fun half(h: Int): Double {
            val exp = (h shr 10) and 0x1f
            val mant = h and 0x3ff
            val v = when (exp) {
                0 -> mant * Math.pow(2.0, -24.0)
                31 -> if (mant == 0) Double.POSITIVE_INFINITY else Double.NaN
                else -> (mant + 1024) * Math.pow(2.0, exp - 25.0)
            }
            return if (h and 0x8000 != 0) -v else v
        }
    }
}

/** Typed reads of decoded CBOR values: null when the value has another type. */
internal fun Any?.asLong(): Long? = this as? Long
internal fun Any?.asInt(): Int? = (this as? Long)?.let { if (it in Int.MIN_VALUE..Int.MAX_VALUE) it.toInt() else null }
internal fun Any?.asBytes(): ByteArray? = this as? ByteArray
internal fun Any?.asString(): String? = this as? String
internal fun Any?.asList(): List<Any?>? = this as? List<Any?>
