package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class CodeTest {
    @get:Rule
    val tmp = TemporaryFolder()

    private fun invite(): String = Node(tmp.newFolder(), "laptop").let { n ->
        try { n.invite(300_000).toString() } finally { n.close() }
    }

    @Test
    fun normalize() {
        assertEquals("K7Q2M9PX", Code.normalize("k7q2-m9px"))
        assertEquals("011V0000", Code.normalize("oilu 0000"))
        assertNull(Code.normalize("K7Q2M9P"))
        assertNull(Code.normalize("K7Q2-M9P!"))
        assertEquals("K7Q2-M9PX", Code.format("K7Q2M9PX"))
    }

    /** The vector of p2pact-go (TestCodeSlotVector). */
    @Test
    fun slotVector() {
        val (slot, key) = Code.slot("k7q2-m9px")!!
        assertEquals("ae1aa553fe39be90efcf1eded8ddf3e6", slot)
        assertEquals("01c23c62071cac48fe6a5cdc7fbf99940219a11f632e3e6565addfbe895c60ca", key.hex())
        // box of "p2pact1.vector" with nonce 00 01 … 0b: decrypts (the invite inside is not a real one)
        val raw = java.util.Base64.getUrlDecoder().decode("AAECAwQFBgcICQoLKQMqysat2QOxFoZEfLdtACgMmw_hEYgh-uCshFLh")
        val c = org.bouncycastle.crypto.modes.ChaCha20Poly1305()
        c.init(false, org.bouncycastle.crypto.params.AEADParameters(org.bouncycastle.crypto.params.KeyParameter(key), 128, raw.copyOf(12), unhex(slot)))
        val out = ByteArray(c.getOutputSize(raw.size - 12))
        c.doFinal(out, c.processBytes(raw, 12, raw.size - 12, out, 0))
        assertEquals("p2pact1.vector", String(out))
    }

    @Test
    fun sealOpenAndTxt() {
        val inv = invite()
        val (slot, box) = Code.seal("K7Q2-M9PX", inv)
        assertEquals(Code.slot("k7q2m9px")!!.first, slot)
        assertEquals(inv, Code.open("k7q2 m9px", box))
        assertNull(Code.open("K7Q2-M9PY", box))
        assertNotEquals(box, Code.seal("K7Q2-M9PX", inv).second)
        // a record as p2pact-go's CodeTXT writes it
        val txt = mutableMapOf("v" to "1", "t" to slot)
        box.chunked(200).forEachIndexed { i, p -> txt["b$i"] = p }
        assertEquals(inv, Code.inviteFromTxt("K7Q2M9PX", txt))
        assertNull(Code.inviteFromTxt("K7Q2M9PY", txt))
        txt.remove("b0")
        assertNull(Code.inviteFromTxt("K7Q2M9PX", txt))
    }

    /** Against a relay with code slots (p2pact-go's cmd/p2pact-relay): P2PACT_RELAY="<host:port> <fingerprint>". */
    @Test
    fun fetchFromRelay() {
        val env = System.getenv("P2PACT_RELAY")
        assumeTrue("P2PACT_RELAY not set", !env.isNullOrBlank())
        val (addr, fp) = env!!.trim().split(Regex("\\s+"))
        val hint = RelayHint(addr, fp)
        val code = (1..8).map { Code.ALPHABET[randomBytes(1)[0].toInt().and(31)] }.joinToString("")
        try { Code.fetch(hint, code); error("found before put") } catch (e: CodeNotFoundException) { }
        val inv = invite()
        val (slot, box) = Code.seal(code, inv)
        Code.slotCall(hint, mapOf("relay" to 1, "op" to "put", "slot" to slot, "box" to box, "ttl" to 60, "tok" to randomBytes(16).hex()), 10_000)
        assertEquals(inv, Code.fetch(hint, Code.format(code.lowercase())))
    }
}
