package io.github.p2pact

import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.Base64

/** Prologue is the protocol version bound into every handshake (spec §10). */
const val PROLOGUE = "p2pact/1"

private val b64e = Base64.getEncoder()
private val b64d = Base64.getDecoder()

internal fun b64(b: ByteArray): String = b64e.encodeToString(b)
internal fun unb64(s: String?): ByteArray? = try { s?.let { b64d.decode(it) } } catch (e: IllegalArgumentException) { null }

/**
 * Identity holds a device's permanent keys (spec §1): X25519 for sessions, Ed25519 for
 * signatures (the family book).
 */
class Identity(val key: KeyPair, val sign: KeyPair) {
    val addr: String = Crypto.address(key.public)

    /** shared derives a pair secret with pub without pairing (devices known from the family book). */
    fun shared(pub: ByteArray, label: String): ByteArray = Crypto.mac(Crypto.x25519(key.private, pub), label)

    companion object {
        /** load reads dir/net-key.json, creating the keys (owner-only) when missing. */
        fun load(dir: File): Identity {
            val file = File(dir, "net-key.json")
            var f: MutableMap<String, Any?> = LinkedHashMap()
            if (file.exists()) {
                @Suppress("UNCHECKED_CAST")
                f = (Json.parse(file.readText()) as? Map<String, Any?>)?.toMutableMap()
                    ?: throw IOException("net-key.json is corrupt")
                val priv = unb64(f["private"] as? String)
                val pub = unb64(f["public"] as? String)
                if (priv?.size != 32 || pub?.size != 32) throw IOException("net-key.json is corrupt")
            }
            var changed = false
            if (f["private"] == null) {
                val k = Crypto.x25519Keypair()
                f["private"] = b64(k.private)
                f["public"] = b64(k.public)
                changed = true
            }
            if (unb64(f["sign"] as? String)?.size != 32) { // keys from a version without signatures
                f["sign"] = b64(randomBytes(32))
                changed = true
            }
            if (changed) {
                dir.mkdirs()
                writeAtomic(file, Json.write(f).toByteArray())
            }
            val key = Crypto.x25519Keypair(unb64(f["private"] as String)!!)
            return Identity(key, Crypto.ed25519Keypair(unb64(f["sign"] as String)!!))
        }
    }
}

/** RelayHint is a relay: its address and the SHA-256 of its certificate (hex). */
data class RelayHint(val addr: String, val fp: String)

/** Hints tell where to find a device (spec §7). */
data class Hints(
    val udp: List<String> = emptyList(),
    val tcp: List<String> = emptyList(),
    val relay: RelayHint? = null,
) {
    /** toWire is [UDP addresses, [relay address, fingerprint], TCP addresses]. */
    fun toWire(): List<Any?> = listOf(
        udp.ifEmpty { null },
        relay?.let { listOf(it.addr, it.fp) },
        tcp.ifEmpty { null },
    )

    internal fun toJson(): Map<String, Any?> = LinkedHashMap<String, Any?>().apply {
        if (udp.isNotEmpty()) put("udp", udp)
        if (tcp.isNotEmpty()) put("tcp", tcp)
        relay?.let { put("relay", mapOf("addr" to it.addr, "fp" to it.fp)) }
    }

    companion object {
        fun fromWire(v: Any?): Hints {
            val a = v.asList() ?: emptyList()
            fun strs(x: Any?) = x.asList()?.mapNotNull { it as? String } ?: emptyList()
            val r = strs(a.getOrNull(1))
            return Hints(
                udp = strs(a.getOrNull(0)),
                tcp = strs(a.getOrNull(2)),
                relay = if (r.size == 2 && r[0].isNotEmpty()) RelayHint(r[0], r[1]) else null,
            )
        }

        internal fun fromJson(v: Any?): Hints {
            val m = v as? Map<*, *> ?: return Hints()
            fun strs(x: Any?) = (x as? List<*>)?.mapNotNull { it as? String } ?: emptyList()
            val r = m["relay"] as? Map<*, *>
            val relay = (r?.get("addr") as? String)?.let { RelayHint(it, r["fp"] as? String ?: "") }
            return Hints(strs(m["udp"]), strs(m["tcp"]), relay)
        }
    }
}

/** Peer is a known device (spec §2): [address, key, name, pair_secret, hints, added]. */
data class Peer(
    val addr: String,
    val key: ByteArray,
    val name: String,
    val pairSecret: ByteArray,
    val hints: Hints,
    val added: Long = 0,
    val signKey: ByteArray? = null,
    val os: String? = null,
    val via: String? = null, // joined through our invite with this id (hex)
) {
    /** relayChannel is the pair's relay channel: only these two devices know it (spec §3). */
    val relayChannel: String get() = Crypto.mac(pairSecret, "relay").copyOf(16).hex()

    override fun equals(other: Any?) = other is Peer && other.addr == addr && same(other.key, key)
    override fun hashCode() = addr.hashCode()
}

/** phone reports a phone OS: a phone is never the family parent. */
fun phone(os: String?) = os == "android" || os == "ios"

/**
 * Trust is the set of known devices in dir/net-peers.json (the same format as p2pact-go). The
 * file is written on a thread of its own, in order: callers never wait for the disk.
 */
class Trust(dir: File) {
    private val file = File(dir, "net-peers.json")
    private val peers = HashMap<String, Peer>()
    private val writer = java.util.concurrent.Executors.newSingleThreadExecutor { r ->
        Thread(r, "p2pact-trust").apply { isDaemon = true }
    }

    init {
        try {
            if (file.exists()) {
                for (j in Json.parse(file.readText()) as? List<*> ?: emptyList<Any?>()) {
                    val m = j as? Map<*, *> ?: continue
                    val key = unb64(m["key"] as? String) ?: continue
                    val p = Peer(
                        addr = m["addr"] as? String ?: continue, key = key, name = m["name"] as? String ?: "",
                        pairSecret = unb64(m["pair_secret"] as? String) ?: ByteArray(0), hints = Hints.fromJson(m["hints"]),
                        added = (m["added"] as? Number)?.toLong() ?: 0, signKey = unb64(m["sign_key"] as? String),
                        os = m["os"] as? String, via = m["via"] as? String,
                    )
                    if (p.key.size == 32 && Crypto.address(p.key) == p.addr) peers[p.addr] = p
                }
            }
        } catch (e: Exception) {
            // none readable: start empty
        }
    }

    @Synchronized
    fun get(addr: String): Peer? = peers[addr]

    @Synchronized
    fun list(): List<Peer> = peers.values.sortedBy { it.addr }

    /** put adds or updates a known device. */
    @Synchronized
    fun put(p: Peer) {
        peers[p.addr] = if (p.added == 0L) p.copy(added = System.currentTimeMillis()) else p
        save()
    }

    /** setHints stores fresh hints of a known device (from its hello). */
    @Synchronized
    fun setHints(addr: String, name: String, os: String, h: Hints) {
        val p = peers[addr] ?: return
        peers[addr] = p.copy(
            name = name.ifEmpty { p.name }, os = os.ifEmpty { p.os },
            hints = Hints(
                udp = h.udp.ifEmpty { p.hints.udp }, tcp = h.tcp.ifEmpty { p.hints.tcp },
                relay = h.relay ?: p.hints.relay,
            ),
        )
        save()
    }

    @Synchronized
    fun remove(addr: String): Boolean {
        if (peers.remove(addr) == null) return false
        save()
        return true
    }

    private fun save() {
        val list = peers.values.sortedBy { it.addr }.map { p ->
            LinkedHashMap<String, Any?>().apply {
                put("addr", p.addr); put("key", b64(p.key)); put("name", p.name); put("pair_secret", b64(p.pairSecret))
                put("hints", p.hints.toJson()); put("added", p.added)
                p.signKey?.let { put("sign_key", b64(it)) }
                p.os?.let { put("os", it) }
                p.via?.let { put("via", it) }
            }
        }
        val b = Json.write(list, "  ").toByteArray()
        writer.execute {
            try {
                file.parentFile?.mkdirs()
                writeAtomic(file, b)
            } catch (e: IOException) {
                // kept in memory; the next change writes it again
            }
        }
    }

    /** flush waits until every change is on disk. */
    fun flush() {
        try { writer.submit {}.get() } catch (e: Exception) { }
    }
}

/**
 * writeAtomic replaces a file in one step: another process (or a restarted node) reading it
 * never sees it half-written.
 */
internal fun writeAtomic(file: File, b: ByteArray) {
    val tmp = File(file.path + ".tmp")
    tmp.writeBytes(b)
    tmp.setReadable(false, false); tmp.setReadable(true, true)
    tmp.setWritable(false, false); tmp.setWritable(true, true)
    Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
}

/** Invite is an invitation to pair (spec §2): `p2pact1.<base64url(CBOR)>`. */
class Invite(val key: ByteArray, val name: String, val secret: ByteArray, val exp: Long /* Unix ms */, val hints: Hints) {
    /** id is the invite id in pairing packets and its relay channel. */
    val id: ByteArray get() = Crypto.mac(secret, "invite").copyOf(16)

    override fun toString(): String {
        val b = Cbor.encode(listOf(1, key, name, secret, exp / 1000, hints.toWire()))
        return PREFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(b)
    }

    companion object {
        private const val PREFIX = "p2pact1."

        /** parse parses an invite string; an expired invite is an error. */
        fun parse(str: String): Invite {
            val s = str.trim()
            require(s.startsWith(PREFIX)) { "not an invite" }
            val v = try {
                Cbor.decode(Base64.getUrlDecoder().decode(s.substring(PREFIX.length).trimEnd('='))).asList()
            } catch (e: Exception) {
                null
            }
            val key = v?.getOrNull(1).asBytes()
            val secret = v?.getOrNull(3).asBytes()
            require(v != null && v[0] == 1L && key?.size == 32 && secret?.size == 32) { "invite is corrupt" }
            val inv = Invite(key!!, v.getOrNull(2).asString() ?: "", secret!!, (v.getOrNull(4).asLong() ?: 0) * 1000,
                Hints.fromWire(v.getOrNull(5)))
            require(System.currentTimeMillis() <= inv.exp) { "invite expired" }
            return inv
        }
    }
}
