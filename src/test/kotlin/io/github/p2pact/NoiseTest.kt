package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Test

class NoiseTest {
    private val prologue = PROLOGUE.toByteArray()

    @Test
    fun ikWithDataInTheFirstMessage() {
        val a = Crypto.x25519Keypair()
        val b = Crypto.x25519Keypair()
        val i = Handshake(Pattern.IK, true, prologue, a, rs = b.public)
        val r = Handshake(Pattern.IK, false, prologue, b)
        val m1 = i.writeMessage("first".toByteArray())
        assertArrayEquals("first".toByteArray(), r.readMessage(m1))
        assertArrayEquals(a.public, r.rs)
        val m2 = r.writeMessage("reply".toByteArray())
        assertArrayEquals("reply".toByteArray(), i.readMessage(m2))
        val (ic1, ic2) = i.ciphers!!
        val (rc1, rc2) = r.ciphers!!
        val ad = byteArrayOf(1)
        assertArrayEquals("x".toByteArray(), rc1.decryptAt(5, ad, ic1.encryptAt(5, ad, "x".toByteArray()), 0))
        assertArrayEquals("y".toByteArray(), ic2.decryptAt(9, ad, rc2.encryptAt(9, ad, "y".toByteArray()), 0))
    }

    @Test
    fun xxPsk2() {
        val a = Crypto.x25519Keypair()
        val b = Crypto.x25519Keypair()
        val psk = randomBytes(32)
        val i = Handshake(Pattern.XXpsk2, true, prologue, a, psk = psk)
        val r = Handshake(Pattern.XXpsk2, false, prologue, b, psk = psk)
        r.readMessage(i.writeMessage())
        i.readMessage(r.writeMessage("owner".toByteArray()))
        assertArrayEquals("joiner".toByteArray(), r.readMessage(i.writeMessage("joiner".toByteArray())))
        assertArrayEquals(b.public, i.rs)
        assertArrayEquals(a.public, r.rs)
    }

    @Test(expected = NoiseException::class)
    fun xxPsk2WrongSecret() {
        val i = Handshake(Pattern.XXpsk2, true, prologue, Crypto.x25519Keypair(), psk = randomBytes(32))
        val r = Handshake(Pattern.XXpsk2, false, prologue, Crypto.x25519Keypair(), psk = randomBytes(32))
        r.readMessage(i.writeMessage())
        i.readMessage(r.writeMessage())
    }

    @Test(expected = NoiseException::class)
    fun ikWrongResponderKey() {
        val i = Handshake(Pattern.IK, true, prologue, Crypto.x25519Keypair(), rs = Crypto.x25519Keypair().public)
        Handshake(Pattern.IK, false, prologue, Crypto.x25519Keypair()).readMessage(i.writeMessage())
    }
}
