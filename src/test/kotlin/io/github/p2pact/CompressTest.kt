package io.github.p2pact

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class CompressTest {
    @get:Rule
    val tmp = TemporaryFolder()

    private val common = Dict(CommonDict.raw)
    private val text = "the agent finished the task and pushed a commit; the tests pass and the build is green.".toByteArray()

    @Test
    fun commonDictionaryId() {
        assertEquals(64 shl 10, CommonDict.raw.size)
        assertEquals("fba2b14e4a7d1458c1cb735ffa51b1c5", common.id.hex())
    }

    @Test
    fun goVectorDecompresses() {
        // made by p2pact-go: compress(text, NewDict(CommonDict), slot 1)
        val v = unhex("010000f5010024026e742066696e697368657461736b75736865646d69743b6573697320677265656e2e08fc705b9b526228108687a5a13989dcc6ec15983f342e57f816")!!
        assertArrayEquals(text, decompress(v, common))
    }

    @Test
    fun roundTripWithAndWithoutDictionary() {
        val z = compress(text, common, 3)!!
        assertEquals(3, z[0].toInt())
        assertTrue(z.size < text.size)
        assertArrayEquals(text, decompress(z, common))
        val big = ("the agent finished the task and pushed a commit. ".repeat(2000)).toByteArray()
        val p = compress(big, null, 0)!!
        assertArrayEquals(big, decompress(p, null))
        val fast = compress(big, common, 1)!! // from 32 KB on: the faster level
        assertArrayEquals(big, decompress(fast, common))
    }

    @Test
    fun notWorthIt() {
        assertNull(compress(ByteArray(63) { 'a'.code.toByte() }, null, 0))
        assertNull(compress(randomBytes(500), common, 1))
    }

    @Test
    fun stripAndRestore() {
        val z = compress(text, common, 1)!!
        val f = restoreFrame(z, 1, common.zid)!!
        assertEquals(0x28, f[0].toInt())
        assertArrayEquals(z.copyOfRange(1, z.size), stripFrame(f))
    }

    @Test(expected = IllegalArgumentException::class)
    fun rawContentDictionaryIsRefused() {
        Dict(ByteArray(1000) { it.toByte() })
    }

    @Test
    fun slotsAreStable() {
        val s = DictStore(tmp.newFolder())
        val peer = "aaaaaaaaaaaaaaaaaaaaaaaaaa"
        s.add(common)
        val own1 = Dict(train(chat(0), emptyList(), 4096), peer, true)
        s.add(own1)
        assertEquals(listOf(common.id.hex(), own1.id.hex()), s.ids(peer).map { it.hex() })
        assertEquals(listOf(common.id.hex()), s.ids("bbbbbbbbbbbbbbbbbbbbbbbbbb").map { it.hex() }) // a pair's is not shown to others
        val (d, slot) = s.pick(peer, listOf(common.id, own1.id), emptyList())
        assertEquals(own1, d)
        assertEquals(2, slot)
        assertEquals(1, s.pick(peer, listOf(common.id, own1.id), listOf(own1.id)).second) // it lacks ours
        // up to four of a kind: the fifth drops the oldest, whose place stays empty
        val more = (1..4).map { Dict(train(chat(it * 100), emptyList(), 4096), peer, true).also { d -> s.add(d) } }
        val ids = s.ids(peer)
        assertEquals(6, ids.size)
        assertEquals(0, ids[1].size)
        assertEquals(more.last().id.hex(), ids.last().hex())
        // reloaded from disk
        val r = DictStore(s.dir.parentFile)
        assertEquals(5, r.dicts.size)
        assertNotNull(r.own(peer))
    }

    companion object {
        fun chat(from: Int, n: Int = 200): List<ByteArray> = (from until from + n).map {
            "step $it: the agent finished the task, ran the tests ${it % 7} times and pushed commit ${it * 7919 % 1000} to the branch".toByteArray()
        }
    }
}
