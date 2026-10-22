package io.github.p2pact

// The Noise Protocol Framework, just what P2PACT uses: Noise_IK and Noise_XXpsk2 with 25519,
// ChaChaPoly and BLAKE2s (spec §2, §4.2).

class NoiseException(msg: String) : Exception(msg)

class CipherState(val k: ByteArray? = null) {
    var n = 0L

    fun encrypt(ad: ByteArray, pt: ByteArray): ByteArray {
        val key = k ?: return pt
        return Crypto.seal(key, n++, ad, pt)
    }

    fun decrypt(ad: ByteArray, ct: ByteArray): ByteArray {
        val key = k ?: return ct
        val pt = Crypto.open(key, n, ad, ct) ?: throw NoiseException("noise: decryption failed")
        n++
        return pt
    }

    /** encryptAt and decryptAt take an explicit nonce: transport packets carry their number. */
    fun encryptAt(n: Long, ad: ByteArray, pt: ByteArray): ByteArray = Crypto.seal(k!!, n, ad, pt)

    fun decryptAt(n: Long, ad: ByteArray, ct: ByteArray, off: Int): ByteArray? = Crypto.open(k!!, n, ad, ct, off)
}

private fun hkdf(ck: ByteArray, ikm: ByteArray, n: Int): List<ByteArray> {
    val t = Crypto.hmac(ck, ikm)
    val o1 = Crypto.hmac(t, byteArrayOf(1))
    val o2 = Crypto.hmac(t, o1, byteArrayOf(2))
    if (n == 2) return listOf(o1, o2)
    return listOf(o1, o2, Crypto.hmac(t, o2, byteArrayOf(3)))
}

private class SymmetricState(name: String) {
    var h: ByteArray
    var ck: ByteArray
    var cs = CipherState()

    init {
        val b = name.toByteArray(Charsets.UTF_8)
        h = if (b.size <= 32) b.copyOf(32) else Crypto.blake2s(b)
        ck = h
    }

    fun mixKey(ikm: ByteArray) {
        val (c, k) = hkdf(ck, ikm, 2)
        ck = c
        cs = CipherState(k)
    }

    fun mixHash(data: ByteArray) {
        h = Crypto.blake2s(h, data)
    }

    fun mixKeyAndHash(ikm: ByteArray) {
        val (c, th, k) = hkdf(ck, ikm, 3)
        ck = c
        mixHash(th)
        cs = CipherState(k)
    }

    fun encryptAndHash(pt: ByteArray): ByteArray = cs.encrypt(h, pt).also { mixHash(it) }

    fun decryptAndHash(ct: ByteArray): ByteArray = cs.decrypt(h, ct).also { mixHash(ct) }

    fun split(): Pair<CipherState, CipherState> {
        val (a, b) = hkdf(ck, ByteArray(0), 2)
        return CipherState(a) to CipherState(b)
    }
}

enum class Pattern(val noiseName: String, val preResponderStatic: Boolean, val msgs: List<List<String>>) {
    IK("IK", true, listOf(listOf("e", "es", "s", "ss"), listOf("e", "ee", "se"))),
    XXpsk2("XXpsk2", false, listOf(listOf("e"), listOf("e", "ee", "s", "es", "psk"), listOf("s", "se"))),
}

/** Handshake runs one Noise handshake. After the last message, [ciphers] holds (c1, c2). */
class Handshake(
    private val pattern: Pattern,
    private val initiator: Boolean,
    prologue: ByteArray,
    private val s: KeyPair,
    rs: ByteArray? = null, // the responder's static key, known in advance (IK initiator)
    private val psk: ByteArray? = null,
) {
    private val ss = SymmetricState("Noise_${pattern.noiseName}_25519_ChaChaPoly_BLAKE2s")
    private var e: KeyPair? = null
    var re: ByteArray? = null
        private set
    var rs: ByteArray? = rs
        private set
    private var i = 0
    var ciphers: Pair<CipherState, CipherState>? = null
        private set

    init {
        ss.mixHash(prologue)
        if (pattern.preResponderStatic) ss.mixHash(if (initiator) rs!! else s.public)
    }

    private val usesPsk get() = psk != null

    private fun dh(a: KeyPair, b: ByteArray) = ss.mixKey(Crypto.x25519(a.private, b))

    fun writeMessage(payload: ByteArray = ByteArray(0)): ByteArray {
        val out = java.io.ByteArrayOutputStream()
        for (t in pattern.msgs[i]) {
            when (t) {
                "e" -> {
                    val k = Crypto.x25519Keypair()
                    e = k
                    out.write(k.public)
                    ss.mixHash(k.public)
                    if (usesPsk) ss.mixKey(k.public)
                }
                "s" -> out.write(ss.encryptAndHash(s.public))
                else -> token(t)
            }
        }
        out.write(ss.encryptAndHash(payload))
        finish()
        return out.toByteArray()
    }

    fun readMessage(msg: ByteArray): ByteArray {
        var pos = 0
        for (t in pattern.msgs[i]) {
            when (t) {
                "e" -> {
                    if (msg.size < pos + 32) throw NoiseException("noise: short message")
                    val r = msg.copyOfRange(pos, pos + 32)
                    pos += 32
                    re = r
                    ss.mixHash(r)
                    if (usesPsk) ss.mixKey(r)
                }
                "s" -> {
                    val n = 32 + if (ss.cs.k != null) 16 else 0
                    if (msg.size < pos + n) throw NoiseException("noise: short message")
                    rs = ss.decryptAndHash(msg.copyOfRange(pos, pos + n))
                    pos += n
                }
                else -> token(t)
            }
        }
        val payload = ss.decryptAndHash(msg.copyOfRange(pos, msg.size))
        finish()
        return payload
    }

    private fun token(t: String) {
        when (t) {
            "ee" -> dh(e!!, re!!)
            "es" -> if (initiator) dh(e!!, rs!!) else dh(s, re!!)
            "se" -> if (initiator) dh(s, re!!) else dh(e!!, rs!!)
            "ss" -> dh(s, rs!!)
            "psk" -> ss.mixKeyAndHash(psk!!)
        }
    }

    private fun finish() {
        i++
        if (i == pattern.msgs.size) ciphers = ss.split()
    }
}
