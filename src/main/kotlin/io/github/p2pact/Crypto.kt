package io.github.p2pact

import org.bouncycastle.crypto.digests.Blake2sDigest
import org.bouncycastle.crypto.macs.HMac
import org.bouncycastle.crypto.modes.ChaCha20Poly1305
import org.bouncycastle.crypto.params.AEADParameters
import org.bouncycastle.crypto.params.KeyParameter
import org.bouncycastle.math.ec.rfc7748.X25519
import org.bouncycastle.math.ec.rfc8032.Ed25519
import java.security.SecureRandom

// Primitives from Bouncy Castle's lightweight API (works the same on Android): X25519, Ed25519,
// ChaCha20-Poly1305, BLAKE2s. No home-made cryptography (spec §1).

internal val random = SecureRandom()

fun randomBytes(n: Int): ByteArray = ByteArray(n).also { random.nextBytes(it) }

class KeyPair(val private: ByteArray, val public: ByteArray)

object Crypto {
    /** x25519Keypair creates a key pair, or derives the public key of a given private key. */
    fun x25519Keypair(priv: ByteArray? = null): KeyPair {
        val p = priv ?: randomBytes(32)
        val pub = ByteArray(32)
        X25519.scalarMultBase(p, 0, pub, 0)
        return KeyPair(p, pub)
    }

    fun x25519(priv: ByteArray, pub: ByteArray): ByteArray {
        val out = ByteArray(32)
        X25519.scalarMult(priv, 0, pub, 0, out, 0)
        return out
    }

    /** ed25519Keypair derives a signing key pair from a 32-byte seed. */
    fun ed25519Keypair(seed: ByteArray): KeyPair {
        val pub = ByteArray(32)
        Ed25519.generatePublicKey(seed, 0, pub, 0)
        return KeyPair(seed, pub)
    }

    fun ed25519Sign(seed: ByteArray, msg: ByteArray): ByteArray {
        val sig = ByteArray(64)
        Ed25519.sign(seed, 0, msg, 0, msg.size, sig, 0)
        return sig
    }

    fun ed25519Verify(pub: ByteArray, msg: ByteArray, sig: ByteArray): Boolean =
        sig.size == 64 && pub.size == 32 && Ed25519.verify(sig, 0, pub, 0, msg, 0, msg.size)

    fun blake2s(vararg parts: ByteArray): ByteArray {
        val d = Blake2sDigest(256)
        for (p in parts) d.update(p, 0, p.size)
        return ByteArray(32).also { d.doFinal(it, 0) }
    }

    fun hmac(key: ByteArray, vararg parts: ByteArray): ByteArray {
        val h = HMac(Blake2sDigest(256))
        h.init(KeyParameter(key))
        for (p in parts) h.update(p, 0, p.size)
        return ByteArray(32).also { h.doFinal(it, 0) }
    }

    /** mac is HMAC-BLAKE2s(key, label): relay channels and invite ids come from secrets. */
    fun mac(key: ByteArray, label: String): ByteArray = hmac(key, label.toByteArray(Charsets.UTF_8))

    private fun nonce(n: Long): ByteArray {
        val b = ByteArray(12)
        for (i in 0 until 8) b[4 + i] = (n ushr (8 * i)).toByte()
        return b
    }

    /** seal is ChaCha20-Poly1305 with the Noise nonce layout (4 zero bytes, 64-bit LE counter). */
    fun seal(key: ByteArray, n: Long, ad: ByteArray, pt: ByteArray): ByteArray {
        val c = ChaCha20Poly1305()
        c.init(true, AEADParameters(KeyParameter(key), 128, nonce(n), ad))
        val out = ByteArray(c.getOutputSize(pt.size))
        val k = c.processBytes(pt, 0, pt.size, out, 0)
        c.doFinal(out, k)
        return out
    }

    /** open reverses seal; null if the tag does not match. */
    fun open(key: ByteArray, n: Long, ad: ByteArray, ct: ByteArray, off: Int = 0, len: Int = ct.size - off): ByteArray? {
        if (len < 16) return null
        return try {
            val c = ChaCha20Poly1305()
            c.init(false, AEADParameters(KeyParameter(key), 128, nonce(n), ad))
            val out = ByteArray(c.getOutputSize(len))
            val k = c.processBytes(ct, off, len, out, 0)
            c.doFinal(out, k)
            out
        } catch (e: Exception) {
            null
        }
    }

    private const val B32 = "abcdefghijklmnopqrstuvwxyz234567"

    /** address of a device: the first 16 bytes of BLAKE2s of its public key, lowercase base32 (spec §1). */
    fun address(pub: ByteArray): String {
        val h = blake2s(pub).copyOf(16)
        val sb = StringBuilder()
        var bits = 0
        var v = 0
        for (b in h) {
            v = (v shl 8) or (b.toInt() and 0xff)
            bits += 8
            while (bits >= 5) {
                sb.append(B32[(v ushr (bits - 5)) and 31])
                bits -= 5
            }
            v = v and ((1 shl bits) - 1)
        }
        if (bits > 0) sb.append(B32[(v shl (5 - bits)) and 31])
        return sb.toString()
    }
}

private val HEX = "0123456789abcdef".toCharArray()

fun ByteArray.hex(): String {
    val sb = StringBuilder(size * 2)
    for (b in this) {
        sb.append(HEX[(b.toInt() shr 4) and 15]).append(HEX[b.toInt() and 15])
    }
    return sb.toString()
}

fun unhex(s: String): ByteArray? {
    if (s.length % 2 != 0) return null
    val out = ByteArray(s.length / 2)
    for (i in out.indices) {
        val hi = Character.digit(s[2 * i], 16)
        val lo = Character.digit(s[2 * i + 1], 16)
        if (hi < 0 || lo < 0) return null
        out[i] = ((hi shl 4) or lo).toByte()
    }
    return out
}

/** Constant-time comparison of byte strings. */
fun same(a: ByteArray, b: ByteArray): Boolean = java.security.MessageDigest.isEqual(a, b)
