package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class CborTest {
    @Test
    fun specExampleIsElevenBytes() {
        // spec §6: a new message "ok" in chat 1
        val b = Cbor.encode(listOf(3, 1, 1, 1, 58, 0, "ok"))
        assertEquals(11, b.size)
        assertEquals("8703010101183a00626f6b", b.hex())
    }

    @Test
    fun roundTrip() {
        val v = listOf(0L, -1L, 23L, 24L, 255L, 256L, 65535L, 65536L, 1L shl 32, -1000L, "текст", byteArrayOf(1, 2, 3),
            listOf<Any?>(), mapOf(64L to 1L), true, false, null, 1.5)
        val d = Cbor.decode(Cbor.encode(v)) as List<*>
        assertEquals(v.size, d.size)
        for (i in v.indices) {
            if (v[i] is ByteArray) assertArrayEquals(v[i] as ByteArray, d[i] as ByteArray) else assertEquals(v[i], d[i])
        }
    }

    @Test
    fun minimalIntegers() {
        assertEquals("17", Cbor.encode(23).hex())
        assertEquals("1818", Cbor.encode(24).hex())
        assertEquals("190100", Cbor.encode(256).hex())
        assertEquals("1a00010000", Cbor.encode(65536).hex())
        assertEquals("20", Cbor.encode(-1).hex())
        assertEquals("1b0000000100000000", Cbor.encode(1L shl 32).hex())
    }

    @Test
    fun goNilSlicesAreNull() {
        // p2pact-go encodes empty hints as [null, null, null]
        val h = Hints.fromWire(Cbor.decode(unhex("83f6f6f6")!!))
        assertTrue(h.udp.isEmpty() && h.tcp.isEmpty())
        assertNull(h.relay)
    }

    @Test
    fun indefiniteAndHalfFloats() {
        assertEquals(listOf(1L, 2L), Cbor.decode(unhex("9f0102ff")!!))
        assertEquals(1.0, Cbor.decode(unhex("f93c00")!!))
        assertEquals("ab", Cbor.decode(unhex("7f61616162ff")!!))
    }

    @Test(expected = Cbor.CborException::class)
    fun truncatedIsAnError() {
        Cbor.decode(unhex("83010203")!!.copyOf(3))
    }

    @Test(expected = Cbor.CborException::class)
    fun hugeLengthIsAnError() {
        Cbor.decode(unhex("9b7fffffffffffffff")!!)
    }
}
