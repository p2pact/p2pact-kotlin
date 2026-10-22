package io.github.p2pact

import com.github.luben.zstd.Zstd
import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDecompressCtx
import com.github.luben.zstd.ZstdDictCompress
import com.github.luben.zstd.ZstdDictDecompress
import com.github.luben.zstd.ZstdException
import java.io.File
import java.io.IOException

// Compression (spec §5): zstd with a dictionary. A compressed message is [dictionary slot 1]
// [zstd frame without its magic and dictionary id]: the receiver knows both from the slot in
// the sender's dictionary list. Slot 0 means no dictionary.

/** ZIP_MIN: shorter messages are not compressed; the gain is below the overhead. */
internal const val ZIP_MIN = 64

/** ZIP_FAST_FROM: from this size on, a faster level — the best one costs more time than the bytes it saves. */
internal const val ZIP_FAST_FROM = 32 shl 10

/** ZIP_QUICK_BELOW: shorter messages with a dictionary at the fastest level (most chat messages). */
internal const val ZIP_QUICK_BELOW = 1 shl 10

/** DICT_KEEP is how many dictionaries of one kind to keep (common, ours for a device, a device's own). */
internal const val DICT_KEEP = 4

/** SLOTS_MAX: past this many slots, a new dictionary takes the oldest empty one. */
internal const val SLOTS_MAX = 32

internal const val ZSTD_DICT_MAGIC = 0xEC30A437L
private val ZSTD_MAGIC = byteArrayOf(0x28, 0xb5.toByte(), 0x2f, 0xfd.toByte())

private const val LEVEL_PLAIN = 19
private const val LEVEL_DICT = 7
private const val LEVEL_QUICK = 1
private const val LEVEL_FAST = 3

/** dictId is a dictionary's id: the first 16 bytes of BLAKE2s of its bytes (spec §5). */
fun dictId(raw: ByteArray): ByteArray = Crypto.blake2s(raw).copyOf(16)

internal fun le32(b: ByteArray, at: Int): Long =
    (b[at].toLong() and 0xff) or ((b[at + 1].toLong() and 0xff) shl 8) or
        ((b[at + 2].toLong() and 0xff) shl 16) or ((b[at + 3].toLong() and 0xff) shl 24)

/**
 * Dict is a compression dictionary in zstd's standard format (magic, a non-zero id, entropy
 * tables, content). peer: "" — a common one, anyone may have it; otherwise it belongs to the
 * pair with that device and is never shown or given to another. own: we trained it (for peer).
 */
class Dict(val raw: ByteArray, val peer: String = "", val own: Boolean = false) {
    val id: ByteArray = dictId(raw)
    /** zid is the dictionary id inside zstd frames (from its header). */
    val zid: Long

    init {
        if (raw.size < 16 || le32(raw, 0) != ZSTD_DICT_MAGIC) throw IllegalArgumentException("not a zstd dictionary")
        zid = le32(raw, 4)
        if (zid == 0L) throw IllegalArgumentException("a zstd dictionary needs an id")
    }

    private var enc: ZstdDictCompress? = null
    private var quick: ZstdDictCompress? = null
    private var fast: ZstdDictCompress? = null
    private var dec: ZstdDictDecompress? = null

    @Synchronized
    internal fun encoder(size: Int): ZstdDictCompress = when {
        size >= ZIP_FAST_FROM -> fast ?: ZstdDictCompress(raw, LEVEL_FAST).also { fast = it }
        size < ZIP_QUICK_BELOW -> quick ?: ZstdDictCompress(raw, LEVEL_QUICK).also { quick = it }
        else -> enc ?: ZstdDictCompress(raw, LEVEL_DICT).also { enc = it }
    }

    @Synchronized
    internal fun decoder(): ZstdDictDecompress = dec ?: ZstdDictDecompress(raw).also { dec = it }

    /** visible reports whether the dictionary may be used with, listed to or given to peer. */
    fun visible(peer: String): Boolean = this.peer.isEmpty() || this.peer == peer

    /** withOwner is the same bytes kept for a pair. */
    internal fun withOwner(peer: String, own: Boolean) = Dict(raw, peer, own)
}

/** One set of zstd contexts per thread: contexts are not thread-safe and costly to create. */
private class Codecs {
    val plain = ZstdCompressCtx()
    val withDict = ZstdCompressCtx()
    val dec = ZstdDecompressCtx()
    val decDict = ZstdDecompressCtx()
}

private val codecs = ThreadLocal.withInitial { Codecs() }

/** compress returns [slot | frame] for a receiver that has d (null — none), or null when not worth it. */
fun compress(msg: ByteArray, d: Dict?, slot: Int): ByteArray? {
    if (msg.size < ZIP_MIN) return null
    val frame = try {
        val c = codecs.get()
        val raw = if (d == null) {
            c.plain.setLevel(if (msg.size >= ZIP_FAST_FROM) LEVEL_FAST else LEVEL_PLAIN)
            c.plain.setChecksum(false)
            c.plain.compress(msg)
        } else {
            val e = d.encoder(msg.size)
            c.withDict.loadDict(e)
            c.withDict.setLevel(if (msg.size >= ZIP_FAST_FROM) LEVEL_FAST else if (msg.size < ZIP_QUICK_BELOW) LEVEL_QUICK else LEVEL_DICT)
            c.withDict.setChecksum(false)
            c.withDict.compress(msg)
        }
        stripFrame(raw)
    } catch (e: Exception) {
        null
    }
    if (frame == null || 1 + frame.size >= msg.size) return null
    val out = ByteArray(1 + frame.size)
    out[0] = slot.toByte()
    System.arraycopy(frame, 0, out, 1, frame.size)
    return out
}

/** decompress reverses compress; d is the dictionary for the slot in the first byte (null for 0). */
fun decompress(b: ByteArray, d: Dict?): ByteArray {
    if (b.size < 2) throw IOException("short")
    val frame = restoreFrame(b, 1, d?.zid ?: 0) ?: throw IOException("bad frame")
    val c = codecs.get()
    val ctx = if (d == null) c.dec else c.decDict.also { it.loadDict(d.decoder()) }
    val size = Zstd.getFrameContentSize(frame)
    if (size in 0..MSG_MAX.toLong()) {
        val out = ByteArray(size.toInt())
        val n = ctx.decompressByteArray(out, 0, out.size, frame, 0, frame.size)
        if (n != out.size) throw IOException("bad frame")
        return out
    }
    if (size >= 0) throw IOException("message too long")
    var cap = maxOf(frame.size * 8, 4096) // unknown size: grow up to MSG_MAX
    while (true) {
        val out = ByteArray(cap)
        try {
            val n = ctx.decompressByteArray(out, 0, out.size, frame, 0, frame.size)
            return out.copyOf(n)
        } catch (e: ZstdException) {
            if (cap >= MSG_MAX) throw IOException("message too long")
            cap = minOf(cap * 4, MSG_MAX)
        }
    }
}

/** stripFrame drops the zstd magic (4 bytes) and the dictionary id (up to 4) from a frame. */
internal fun stripFrame(f: ByteArray): ByteArray? {
    if (f.size < 6 || !(0 until 4).all { f[it] == ZSTD_MAGIC[it] }) return null
    val fhd = f[4].toInt() and 0xff
    val didLen = intArrayOf(0, 1, 2, 4)[fhd and 3]
    var at = 5
    if (fhd and 0x20 == 0) at++ // not single-segment: a window descriptor follows
    if (f.size < at + didLen) return null
    val out = ByteArray(f.size - 4 - didLen)
    out[0] = (fhd and 3.inv()).toByte()
    System.arraycopy(f, 5, out, 1, at - 5)
    System.arraycopy(f, at + didLen, out, at - 4, f.size - at - didLen)
    return out
}

/** restoreFrame puts back the magic and dictionary id zid (0 — no dictionary); the frame starts at off. */
internal fun restoreFrame(b: ByteArray, off: Int, zid: Long): ByteArray? {
    val len = b.size - off
    if (len < 2) return null
    val fhd = b[off].toInt() and 0xff
    if (fhd and 3 != 0) return null
    var at = 1
    if (fhd and 0x20 == 0) at++
    if (len < at) return null
    if (zid == 0L) return cat(ZSTD_MAGIC, b.copyOfRange(off, b.size))
    val out = ByteArray(4 + len + 4)
    System.arraycopy(ZSTD_MAGIC, 0, out, 0, 4)
    out[4] = (fhd or 3).toByte()
    System.arraycopy(b, off + 1, out, 5, at - 1)
    for (i in 0 until 4) out[4 + at + i] = (zid ushr (8 * i)).toByte()
    System.arraycopy(b, off + at, out, 8 + at, len - at)
    return out
}

internal fun hasId(list: List<ByteArray>, id: ByteArray) = list.any { same(it, id) }

/**
 * DictStore keeps dictionaries in dir/net-dicts/<id hex>; the order (newest first) is in the
 * file "order", a line per dictionary: "<id hex>" for a common one, "<id hex> <device> own|their"
 * for a pair's (the same layout as p2pact-go).
 */
internal class DictStore(dir: File) {
    val dir = File(dir, "net-dicts")
    var dicts: List<Dict> = emptyList() // newest first
        private set

    // slots: what we list to each device, by slot. A dictionary keeps its slot while listed;
    // a dropped one leaves an empty id, and a new one goes to the end.
    private val slots = HashMap<String, MutableList<ByteArray>>()
    private var gen = 0 // bumped on every change
    private val cache = HashMap<String, Pair<Int, List<ByteArray>>>()

    init {
        val order = try { File(this.dir, "order").readText() } catch (e: IOException) { "" }
        val out = ArrayList<Dict>()
        for (line in order.split("\n")) {
            val f = line.split(Regex("\\s+")).filter { it.isNotEmpty() }
            if (f.size != 1 && f.size != 3) continue // a damaged line: never take a pair's dictionary for a common one
            try {
                val raw = File(this.dir, f[0]).readBytes()
                val d = if (f.size == 3) Dict(raw, f[1], f[2] == "own") else Dict(raw)
                if (d.id.hex() == f[0]) out.add(d)
            } catch (e: Exception) {
                // missing or damaged: skip
            }
        }
        dicts = out
    }

    /** get returns dictionary id if peer may see it. */
    fun get(id: ByteArray, peer: String): Dict? = dicts.firstOrNull { same(it.id, id) && it.visible(peer) }

    /** add puts a dictionary first (moving it up if known) and deletes the oldest extra ones of its kind. */
    fun add(d: Dict) {
        dir.mkdirs()
        val f = File(dir, d.id.hex())
        if (!f.exists() || f.length() != d.raw.size.toLong()) writeAtomic(f, d.raw)
        val out = arrayListOf(d)
        val dropped = ArrayList<Dict>()
        val kinds = HashMap<Pair<String, Boolean>, Int>()
        kinds[d.peer to d.own] = 1
        for (x in dicts) {
            if (same(x.id, d.id) && x.peer == d.peer) continue
            val k = x.peer to x.own
            val n = (kinds[k] ?: 0) + 1
            kinds[k] = n
            if (n > DICT_KEEP) dropped.add(x) else out.add(x)
        }
        for (x in dropped) { // one file serves the same bytes kept as another kind
            if (out.none { same(it.id, x.id) }) File(dir, x.id.hex()).delete()
        }
        dicts = out
        gen++
        saveOrder()
    }

    /** saveOrder writes "order" through a temporary file: a torn one could turn a pair's dictionary into a common one. */
    private fun saveOrder() {
        val lines = dicts.joinToString("") { x ->
            x.id.hex() + (if (x.peer.isNotEmpty()) " ${x.peer} ${if (x.own) "own" else "their"}" else "") + "\n"
        }
        writeAtomic(File(dir, "order"), lines.toByteArray())
    }

    /** list is what peer sees of our dictionaries, newest first: the common ones and the pair's. */
    fun list(peer: String): List<Dict> {
        val out = ArrayList<Dict>()
        for (d in dicts) if (d.visible(peer) && out.none { same(it.id, d.id) }) out.add(d)
        return out
    }

    /** ids is our list for peer, by slot (an empty id — a free slot). */
    fun ids(peer: String): List<ByteArray> = ArrayList(slotsFor(peer))

    private fun slotsFor(peer: String): List<ByteArray> {
        cache[peer]?.let { if (it.first == gen) return it.second }
        val list = list(peer)
        val s = slots.getOrPut(peer) { ArrayList() }
        for (i in s.indices) if (s[i].isNotEmpty() && list.none { same(it.id, s[i]) }) s[i] = ByteArray(0)
        for (j in list.indices.reversed()) { // the oldest first, so the newest gets the last slot
            val d = list[j]
            if (hasId(s, d.id)) continue
            val free = if (s.size >= SLOTS_MAX) s.indexOfFirst { it.isEmpty() } else -1
            if (free < 0) s.add(d.id) else s[free] = d.id
        }
        val snapshot = ArrayList(s)
        cache[peer] = gen to snapshot
        return snapshot
    }

    /** slotOf is d's slot in our list for peer (from 1). */
    fun slotOf(peer: String, d: Dict): Int = slotsFor(peer).indexOfFirst { same(it, d.id) } + 1

    /** own returns our newest dictionary trained for peer. */
    fun own(peer: String): Dict? = dicts.firstOrNull { it.own && it.peer == peer }

    /**
     * pick returns the dictionary to compress for peer and its slot in our list for peer: ours
     * trained for it if it has it, else the newest it also has (its list minus lacks).
     */
    fun pick(peer: String, theirs: List<ByteArray>, lacks: List<ByteArray>): Pair<Dict?, Int> {
        val list = list(peer)
        fun ok(d: Dict) = hasId(theirs, d.id) && !hasId(lacks, d.id)
        val d = list.firstOrNull { it.own && ok(it) } ?: list.firstOrNull { ok(it) } ?: return null to 0
        return d to slotOf(peer, d)
    }

    /** forget drops what belongs to the pair with peer: our dictionaries for it and its own. */
    fun forget(peer: String) {
        val drop = dicts.filter { it.peer == peer }
        if (drop.isEmpty()) return
        dicts = dicts.filter { it.peer != peer }
        for (d in drop) if (dicts.none { same(it.id, d.id) }) File(dir, d.id.hex()).delete()
        slots.remove(peer)
        cache.remove(peer)
        gen++
        try { saveOrder() } catch (e: IOException) { }
    }
}

/** COMMON_DICT is the common dictionary (spec §5, dict/common-1.zdict of the spec repository). */
object CommonDict {
    val raw: ByteArray by lazy {
        CommonDict::class.java.getResourceAsStream("/io/github/p2pact/common-1.zdict")!!.use { it.readBytes() }
    }
}
