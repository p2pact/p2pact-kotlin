package io.github.p2pact

import com.github.luben.zstd.Zstd
import java.io.File
import java.io.FileOutputStream
import java.util.concurrent.ExecutorService

// Our own dictionaries (spec §5): a device trains a dictionary for each device it talks to, on
// that conversation, so the dictionary shows the other side nothing it has not already seen.
// It learns what it sent; while that is little, what it got fills the history up to 256 KB.
// The first dictionary comes at 32 KB of history; then a new one when the history has doubled
// (up to 1 MB), monthly, or when compression got 15 % worse than right after training.

internal const val LEARN_FIRST = 32 shl 10 // history to train the first dictionary on
internal const val LEARN_MAX = 1 shl 20 // our messages kept: the newest
internal const val LEARN_HEARD = 256 shl 10 // the received ones fill the history up to this
internal const val LEARN_SAMPLE = 4 shl 10 // a message is cut to this: a dictionary helps its start most
internal const val LEARN_WINDOW = 32 shl 10 // bytes sent to measure compression over
internal const val LEARN_WORSE = 1.15 // retrain when compression is this much worse than the first window
internal const val LEARN_MONTH = 30L * 24 * 3600 * SEC
internal const val LEARN_DICT_MAX = 64 shl 10
/** LEARN_QUEUE_MAX: bytes waiting for the history; past it messages are skipped. */
internal const val LEARN_QUEUE_MAX = 1 shl 20
/** LEARN_OWN_MIN: a device that sends (almost) nothing worth compressing trains no dictionary. */
internal const val LEARN_OWN_MIN = 8

/**
 * LearnTiming: learning is deferred. Sending and receiving only queue the message; once a tick
 * the queue goes into the history, off the sending path, and training waits for a lull in the
 * conversation (no message for lull, or after waiting wait). Tests shorten them.
 */
class LearnTiming(
    val tickMs: Long = 1000,
    val lullMs: Long = 1000,
    val waitMs: Long = 60_000,
    val checkMs: Long = 24 * 3600_000L,
)

/** History is one conversation, kept in dir/net-history/<hash of the device's address>. */
internal class History(val file: File) {
    var fileSize = 0L
    var buf = Bytes(0) // records not yet written: the disk is written off the node's thread
    var rewrite = false // the file has outgrown the history: write it anew
    var flushing = false
    var gone = false // the device was forgotten: write nothing more
    val own = ArrayDeque<ByteArray>()
    val heard = ArrayDeque<ByteArray>()
    var ownSize = 0
    var heardSize = 0
    var trained = 0 // history size when the current dictionary was trained
    var at = 0L // when it was trained (Unix ms)
    var plain = 0L // compression with the current dictionary over the current window
    var zips = 0L
    var base = 0.0 // and over the first window after training
    var worse = false // a window came out 15 % worse than the first
    var checked = 0L // the last monthly and 15 % check (Unix ms)
    var due = "" // a new dictionary is due ("" — not), waiting for a lull since dueAt
    var dueAt = 0L
    var training = false

    /** size is how much the next dictionary would be trained on. */
    fun size(): Int = ownSize + minOf(heardSize, maxOf(0, LEARN_HEARD - ownSize))

    fun add(m0: ByteArray, isHeard: Boolean) {
        val m = if (m0.size > LEARN_SAMPLE) m0.copyOf(LEARN_SAMPLE) else m0
        if (isHeard) {
            heard.addLast(m)
            heardSize += m.size
            while (heardSize > LEARN_HEARD) heardSize -= heard.removeFirst().size
        } else {
            own.addLast(m)
            ownSize += m.size
            while (ownSize > LEARN_MAX) ownSize -= own.removeFirst().size
        }
    }
}

internal fun historyFile(dir: File, peer: String): File =
    File(File(dir, "net-history"), Crypto.blake2s(peer.toByteArray()).copyOf(8).hex())

/** record is a message in the file: [length 4][1 — received, 0 — sent][bytes]. */
private fun record(b: Bytes, m: ByteArray, heard: Boolean) {
    b.bytes(be32(m.size.toLong()))
    b.byte(if (heard) 1 else 0)
    b.bytes(m)
}

private class Pending(val msg: ByteArray, val heard: Boolean, val plain: Int, val zips: Int)

private class PeerLearn {
    var queue = ArrayList<Pending>()
    var queued = 0
    var lastTalk = 0L // Unix ms
    var hist: History? = null
}

/**
 * Learner keeps the conversation histories and trains our dictionary for each device. Its
 * methods run on the node's thread; the disk is written on [io] and dictionaries are trained
 * on [trainer], one at a time; their results come back through [post].
 */
internal class Learner(
    private val dir: File,
    private val dicts: DictStore,
    private val timing: LearnTiming,
    private val io: ExecutorService,
    private val trainer: ExecutorService,
    private val post: (() -> Unit) -> Unit,
    private val onDict: (Dict) -> Boolean,
) {
    private val peers = HashMap<String, PeerLearn>()
    @Volatile var closed = false

    /** hear queues a conversation message we got from the device. */
    fun hear(peer: String, msg: ByteArray) = queue(peer, Pending(msg, true, 0, 0))

    /** talk queues a conversation message we send; d and z: the dictionary and the result (null — sent as is). */
    fun talk(peer: String, msg: ByteArray, d: Dict?, z: ByteArray?) {
        val own = d?.own == true
        queue(peer, Pending(msg, false, if (own) msg.size else 0, if (own) (z?.size ?: msg.size) else 0))
    }

    private fun queue(peer: String, p: Pending) {
        if (p.msg.size < ZIP_MIN) return
        val pl = peers.getOrPut(peer) { PeerLearn() }
        if (pl.queued >= LEARN_QUEUE_MAX) return
        pl.queue.add(p)
        pl.queued += minOf(p.msg.size, LEARN_SAMPLE)
        pl.lastTalk = System.currentTimeMillis()
    }

    /** tick takes the queues into the histories and trains when it is time. */
    fun tick(now: Long = System.currentTimeMillis()) {
        for ((peer, pl) in peers) {
            if (pl.queue.isEmpty() && pl.hist?.due.isNullOrEmpty()) continue
            val q = pl.queue
            pl.queue = ArrayList()
            pl.queued = 0
            val h = history(peer, pl)
            for (p in q) {
                val m = if (p.msg.size > LEARN_SAMPLE) p.msg.copyOf(LEARN_SAMPLE) else p.msg
                h.add(m, p.heard)
                save(h, m, p.heard)
                h.plain += p.plain
                h.zips += p.zips
            }
            consider(peer, pl, h, now)
        }
    }

    private fun history(peer: String, pl: PeerLearn): History {
        pl.hist?.let { return it }
        val h = History(historyFile(dir, peer))
        pl.hist = h
        val b = try { h.file.readBytes() } catch (e: Exception) { ByteArray(0) }
        h.fileSize = b.size.toLong()
        var at = 0
        while (at + 5 <= b.size) {
            val l = u32(b, at)
            if (l > LEARN_SAMPLE || at + 5 + l > b.size || b[at + 4] > 1) break // a torn record: the rest is lost
            h.add(b.copyOfRange(at + 5, at + 5 + l.toInt()), b[at + 4].toInt() == 1)
            at += 5 + l.toInt()
        }
        dicts.own(peer)?.let { d ->
            h.trained = h.size()
            h.at = File(dicts.dir, d.id.hex()).lastModified()
        }
        return h
    }

    /** save queues a record for the file; when the file has outgrown the history twice, it is written anew. */
    private fun save(h: History, m: ByteArray, heard: Boolean) {
        record(h.buf, m, heard)
        if (h.fileSize + h.buf.size > 2L * (LEARN_MAX + LEARN_HEARD)) h.rewrite = true
        if (!h.flushing) {
            h.flushing = true
            flush(h)
        }
    }

    /** flush takes what waits for the file (on the node's thread) and writes it on the io thread. */
    private fun flush(h: History) {
        val whole = h.rewrite
        val data = if (whole) {
            val b = Bytes(h.ownSize + h.heardSize + 5 * (h.own.size + h.heard.size))
            for (x in h.own) record(b, x, false)
            for (x in h.heard) record(b, x, true)
            b
        } else h.buf
        h.buf = Bytes(0)
        h.rewrite = false
        if (data.size == 0 || h.gone || closed) {
            h.flushing = false
            return
        }
        val bytes = data.toByteArray()
        io.execute {
            try {
                h.file.parentFile?.mkdirs()
                if (whole) writeAtomic(h.file, bytes)
                else FileOutputStream(h.file, true).use { it.write(bytes) }
                if (h.gone) h.file.delete() // forgotten while writing
            } catch (e: Exception) {
                // the history lives on in memory
            }
            post {
                h.fileSize = if (whole) bytes.size.toLong() else h.fileSize + bytes.size
                flush(h)
            }
        }
    }

    /** consider decides whether a new dictionary is due and trains it in a lull. */
    private fun consider(peer: String, pl: PeerLearn, h: History, now: Long) {
        if (h.training) return
        val own = dicts.own(peer)
        val size = h.size()
        if (h.plain >= LEARN_WINDOW) { // compression over the last window of what we sent with it
            val ratio = h.zips.toDouble() / h.plain
            h.plain = 0
            h.zips = 0
            if (h.base == 0.0) h.base = ratio else if (ratio >= h.base * LEARN_WORSE) h.worse = true
        }
        if (h.due.isEmpty() && h.own.size >= LEARN_OWN_MIN) {
            when {
                own == null -> if (size >= maxOf(LEARN_FIRST, 2 * h.trained)) h.due = "first" // trained: a try that failed
                size < LEARN_FIRST -> {} // too little to replace what we have (the history was lost, say)
                h.trained < LEARN_MAX && size >= 2 * h.trained -> h.due = "doubled"
                now - h.checked >= timing.checkMs -> {
                    h.checked = now
                    if ((now - h.at) * MS > LEARN_MONTH || h.worse) h.due = "check"
                }
            }
            if (h.due.isNotEmpty()) h.dueAt = now
        }
        if (h.due.isEmpty() || (now - pl.lastTalk < timing.lullMs && now - h.dueAt < timing.waitMs)) return
        h.due = ""
        h.worse = false
        h.training = true
        val ownMsgs = ArrayList(h.own)
        val heardAll = ArrayList(h.heard)
        val heard = heardAll.subList(heardAll.size - countFrom(heardAll, maxOf(0, LEARN_HEARD - h.ownSize)), heardAll.size).toList()
        val dictSize = minOf(LEARN_DICT_MAX, maxOf(size / 4, 4 shl 10))
        trainer.execute {
            val raw = if (closed) null else try { train(ownMsgs, heard, dictSize) } catch (e: Throwable) { null }
            post {
                // a failure waits like a success: until the history doubles or the next check
                h.training = false
                h.trained = size
                h.at = System.currentTimeMillis()
                if (closed || raw == null || peers[peer] !== pl) return@post
                val d = try { Dict(raw, peer, true) } catch (e: Exception) { return@post }
                if (onDict(d)) {
                    h.plain = 0
                    h.zips = 0
                    h.base = 0.0
                }
            }
        }
    }

    /** training tells whether a new dictionary for peer is due or being trained, or messages wait to be learnt. */
    fun training(peer: String): Boolean {
        val pl = peers[peer] ?: return false
        val h = pl.hist
        return pl.queue.isNotEmpty() || (h != null && (h.training || h.due.isNotEmpty()))
    }

    /** forget drops the conversation with peer: its history in memory and on disk. */
    fun forget(peer: String) {
        peers.remove(peer)?.hist?.gone = true
        val f = historyFile(dir, peer)
        io.execute { f.delete() }
    }

    /** historySize is how much of what we sent and got the history of peer holds (tests). */
    fun historySize(peer: String): Pair<Int, Int> {
        val h = history(peer, peers.getOrPut(peer) { PeerLearn() })
        return h.ownSize to h.heardSize
    }

    /** flushed tells whether everything learnt so far is on disk (tests). */
    fun flushed(peer: String): Boolean {
        val h = peers[peer]?.hist ?: return true
        return !h.flushing && h.buf.size == 0
    }
}

/** countFrom is how many of the newest messages fit in n bytes. */
internal fun countFrom(msgs: List<ByteArray>, n0: Int): Int {
    var n = n0
    var c = 0
    var i = msgs.size - 1
    while (i >= 0 && n >= msgs[i].size) {
        n -= msgs[i].size
        c++
        i--
    }
    return c
}

/**
 * train trains a zstd dictionary of up to size bytes with zstd's own trainer (ZDICT, fastCover:
 * the algorithm of `zstd --train`), which tries several segment sizes on three quarters of the
 * samples and keeps the content that compresses the rest best. The samples are ordered so the
 * rest is our newest messages: the dictionary is for what we send. The result is in zstd's
 * standard format (magic, id, entropy tables, content), as spec §5 requires.
 */
internal fun train(own: List<ByteArray>, heard: List<ByteArray>, size: Int): ByteArray {
    val samples = (heard + own).filter { it.isNotEmpty() }
    if (samples.size < 8) throw IllegalArgumentException("too few samples")
    val buf = ByteArray(size)
    val n = Zstd.trainFromBuffer(samples.toTypedArray(), buf, false)
    if (Zstd.isError(n)) throw IllegalStateException("training: " + Zstd.getErrorName(n))
    val raw = buf.copyOf(n.toInt())
    if (raw.size < 16 || le32(raw, 0) != ZSTD_DICT_MAGIC || le32(raw, 4) == 0L) throw IllegalStateException("training: not a dictionary")
    return raw
}
