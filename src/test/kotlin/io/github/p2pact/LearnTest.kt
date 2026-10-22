package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.util.concurrent.Executors

class LearnTest {
    @get:Rule
    val tmp = TemporaryFolder()

    @Test
    fun trainsAStandardDictionary() {
        val raw = train(CompressTest.chat(0), CompressTest.chat(1000, 50), 8 shl 10)
        assertEquals(ZSTD_DICT_MAGIC, le32(raw, 0))
        assertTrue(le32(raw, 4) != 0L)
        val d = Dict(raw, "x", true)
        val msg = "step 77: the agent finished the task, ran the tests 3 times and pushed commit 123 to the branch".toByteArray()
        val z = compress(msg, d, 1)!!
        assertTrue(z.size < compress(msg, null, 0)?.size ?: msg.size)
        assertEquals(String(msg), String(decompress(z, d)))
    }

    private class Harness(dir: java.io.File) {
        val io = Executors.newSingleThreadExecutor()
        val trainer = Executors.newSingleThreadExecutor()
        val loop = Executors.newSingleThreadExecutor()
        val dicts = DictStore(dir)
        val got = ArrayList<Dict>()
        val learner = Learner(dir, dicts, LearnTiming(lullMs = 0, waitMs = 0), io, trainer, { f -> loop.execute(f) }) { d ->
            dicts.add(d); got.add(d); true
        }

        fun <T> on(f: () -> T): T = loop.submit(f).get()

        fun settle() {
            repeat(50) {
                Thread.sleep(50)
                on { learner.tick(System.currentTimeMillis() + 10_000) }
            }
        }
    }

    @Test
    fun ownDictionaryAt32KB() {
        val h = Harness(tmp.newFolder())
        val peer = "cccccccccccccccccccccccccc"
        h.on { for (m in CompressTest.chat(0, 400)) h.learner.talk(peer, m, null, null) }
        h.settle()
        assertEquals(1, h.on { h.got.size })
        assertNotNull(h.on { h.dicts.own(peer) })
        assertFalse(h.on { h.learner.training(peer) })
        // the history is on disk and comes back
        assertTrue(historyFile(h.dicts.dir.parentFile, peer).length() > 32 shl 10)
    }

    @Test
    fun aDeviceThatOnlyReceivesTrainsNone() {
        val h = Harness(tmp.newFolder())
        val peer = "dddddddddddddddddddddddddd"
        h.on {
            for (m in CompressTest.chat(0, 600)) h.learner.hear(peer, m)
            for (m in CompressTest.chat(0, 7)) h.learner.talk(peer, m, null, null) // fewer than 8 of its own
        }
        h.settle()
        assertTrue(h.on { h.got.isEmpty() })
        assertNull(h.on { h.dicts.own(peer) })
        h.on { h.learner.talk(peer, CompressTest.chat(900, 1)[0], null, null) } // the 8th
        h.settle()
        assertEquals(1, h.on { h.got.size })
    }

    @Test
    fun shortMessagesAreNotLearnt() {
        val h = Harness(tmp.newFolder())
        val peer = "eeeeeeeeeeeeeeeeeeeeeeeeee"
        h.on { repeat(2000) { h.learner.talk(peer, ByteArray(63) { 'a'.code.toByte() }, null, null) } }
        h.settle()
        assertEquals(0 to 0, h.on { h.learner.historySize(peer) })
    }
}
