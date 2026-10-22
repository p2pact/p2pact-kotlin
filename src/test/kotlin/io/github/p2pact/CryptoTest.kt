package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class CryptoTest {
    @Test
    fun blake2sVector() {
        assertEquals("69217a3079908094e11121d042354a7c1f55b6482ca1a51e1b250dfd1ed0eef9", Crypto.blake2s(ByteArray(0)).hex())
        assertEquals("508c5e8c327c14e2e1a72ba34eeb452f37458b209ed63a294d999b4c86675982", Crypto.blake2s("abc".toByteArray()).hex())
    }

    @Test
    fun address() {
        val k = Crypto.x25519Keypair()
        val a = Crypto.address(k.public)
        assertEquals(26, a.length)
        assertTrue(a.all { it in 'a'..'z' || it in '2'..'7' })
    }

    @Test
    fun x25519Agrees() {
        val a = Crypto.x25519Keypair()
        val b = Crypto.x25519Keypair()
        assertArrayEquals(Crypto.x25519(a.private, b.public), Crypto.x25519(b.private, a.public))
    }

    @Test
    fun x25519Rfc7748() {
        val priv = unhex("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")!!
        assertEquals("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a", Crypto.x25519Keypair(priv).public.hex())
    }

    @Test
    fun ed25519() {
        val k = Crypto.ed25519Keypair(randomBytes(32))
        val sig = Crypto.ed25519Sign(k.private, "hello".toByteArray())
        assertTrue(Crypto.ed25519Verify(k.public, "hello".toByteArray(), sig))
        assertFalse(Crypto.ed25519Verify(k.public, "hellO".toByteArray(), sig))
    }

    @Test
    fun sealOpen() {
        val key = randomBytes(32)
        val ad = byteArrayOf(6, 0, 0, 0, 1)
        val ct = Crypto.seal(key, 7, ad, "data".toByteArray())
        assertEquals(4 + 16, ct.size)
        assertArrayEquals("data".toByteArray(), Crypto.open(key, 7, ad, ct))
        assertNull(Crypto.open(key, 8, ad, ct))
        assertNull(Crypto.open(key, 7, byteArrayOf(6), ct))
    }

    @Test
    fun hmacBlake2sDiffersByLabel() {
        val k = randomBytes(32)
        assertEquals(32, Crypto.mac(k, "relay").size)
        assertFalse(same(Crypto.mac(k, "relay"), Crypto.mac(k, "invite")))
    }
}
