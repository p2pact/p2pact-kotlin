package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.io.File

class IdentityTest {
    @get:Rule
    val tmp = TemporaryFolder()

    @Test
    fun identityPersists() {
        val dir = tmp.newFolder()
        val a = Identity.load(dir)
        val b = Identity.load(dir)
        assertEquals(a.addr, b.addr)
        assertArrayEquals(a.sign.public, b.sign.public)
        assertTrue(File(dir, "net-key.json").readText().contains("\"sign\""))
    }

    @Test
    fun identityFromGoWithoutSigningKeyGetsOne() {
        val dir = tmp.newFolder()
        val k = Crypto.x25519Keypair()
        File(dir, "net-key.json").writeText(Json.write(mapOf("private" to b64(k.private), "public" to b64(k.public))))
        val id = Identity.load(dir)
        assertEquals(Crypto.address(k.public), id.addr)
        assertEquals(id.sign.public.hex(), Identity.load(dir).sign.public.hex())
    }

    @Test
    fun sharedSecretIsSymmetric() {
        val a = Identity.load(tmp.newFolder())
        val b = Identity.load(tmp.newFolder())
        assertArrayEquals(a.shared(b.key.public, "family-pair"), b.shared(a.key.public, "family-pair"))
    }

    @Test
    fun inviteRoundTrip() {
        val k = Crypto.x25519Keypair()
        val inv = Invite(k.public, "Work laptop", randomBytes(32), System.currentTimeMillis() + 600_000,
            Hints(udp = listOf("192.168.1.2:48730"), relay = RelayHint("relay.example:8443", "ab12")))
        val s = inv.toString()
        assertTrue(s.startsWith("p2pact1."))
        assertFalse(s.contains("="))
        val p = Invite.parse(s)
        assertArrayEquals(inv.key, p.key)
        assertArrayEquals(inv.secret, p.secret)
        assertArrayEquals(inv.id, p.id)
        assertEquals("Work laptop", p.name)
        assertEquals(inv.exp / 1000 * 1000, p.exp)
        assertEquals(inv.hints, p.hints)
    }

    @Test(expected = IllegalArgumentException::class)
    fun expiredInvite() {
        Invite.parse(Invite(Crypto.x25519Keypair().public, "x", randomBytes(32), System.currentTimeMillis() - 1000, Hints()).toString())
    }

    @Test(expected = IllegalArgumentException::class)
    fun corruptInvite() {
        Invite.parse("p2pact1.AAAA")
    }

    @Test
    fun trustRoundTripInGoFormat() {
        val dir = tmp.newFolder()
        val k = Crypto.x25519Keypair()
        val addr = Crypto.address(k.public)
        val t = Trust(dir)
        t.put(Peer(addr, k.public, "Phone", randomBytes(32), Hints(udp = listOf("10.0.0.2:1")), os = "android"))
        t.setHints(addr, "Phone 2", "", Hints(tcp = listOf("10.0.0.2:2")))
        t.flush()
        val json = File(dir, "net-peers.json").readText()
        assertTrue(json.contains("\"pair_secret\""))
        val u = Trust(dir).get(addr)
        assertNotNull(u)
        assertEquals("Phone 2", u!!.name)
        assertEquals(listOf("10.0.0.2:1"), u.hints.udp)
        assertEquals(listOf("10.0.0.2:2"), u.hints.tcp)
        assertEquals("android", u.os)
        assertTrue(u.added > 0)
        assertTrue(t.remove(addr))
        t.flush()
        assertNull(Trust(dir).get(addr))
    }

    @Test
    fun trustSkipsAForgedAddress() {
        val dir = tmp.newFolder()
        val k = Crypto.x25519Keypair()
        File(dir, "net-peers.json").writeText("""[{"addr":"aaaaaaaaaaaaaaaaaaaaaaaaaa","key":"${b64(k.public)}","name":"x","pair_secret":"","hints":{},"added":1}]""")
        assertTrue(Trust(dir).list().isEmpty())
    }

    @Test
    fun window() {
        val w = Window()
        assertTrue(w.accept(0))
        assertFalse(w.accept(0))
        assertTrue(w.accept(5))
        assertTrue(w.accept(3))
        assertFalse(w.accept(3))
        assertTrue(w.accept(5000))
        assertFalse(w.accept(5000 - 2048))
        assertTrue(w.accept(5000 - 2047))
        assertFalse(w.accept(5))
    }
}
