package io.github.p2pact

import org.bouncycastle.crypto.digests.SHA256Digest
import org.bouncycastle.crypto.generators.PKCS5S2ParametersGenerator
import org.bouncycastle.crypto.modes.ChaCha20Poly1305
import org.bouncycastle.crypto.params.AEADParameters
import org.bouncycastle.crypto.params.KeyParameter
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.security.MessageDigest
import java.security.cert.X509Certificate
import java.util.Base64
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/** The relay has no code slots (an older relay): look on the local network only. */
class CodeUnsupportedException : Exception("relay has no code slots")

/** Nobody shows this code now (wrong, expired or already used). */
class CodeNotFoundException : Exception("code not found")

/**
 * Short code (spec §2): an invite a person can type. The inviting device seals a fresh invite
 * under an 8-symbol code and puts it on its relay (code slots, spec §3) and in its `_p2pact._udp`
 * record (spec §7); given the code, [fetch] gets it from a relay and [inviteFromTxt] from a
 * discovery record (browsing is the platform's: NsdManager on Android). The code is low-entropy:
 * it only finds the invite; the inviting device asks its person before granting anything.
 */
object Code {
    const val ALPHABET = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    const val LENGTH = 8

    /** SERVICE is the DNS-SD service type of local discovery (spec §7). */
    const val SERVICE = "_p2pact._udp"

    private const val SALT = "p2pact-code"
    private const val ROUNDS = 100_000

    /** normalize returns a typed code in canonical form (upper case, no dashes, O→0, I/L→1, U→V); null — not a code. */
    fun normalize(s: String): String? {
        val b = StringBuilder()
        for (ch in s.uppercase()) {
            val r = when (ch) {
                ' ', '-', '\t', '\n', '·' -> continue
                'O' -> '0'
                'I', 'L' -> '1'
                'U' -> 'V'
                else -> ch
            }
            if (r !in ALPHABET) return null
            b.append(r)
        }
        return if (b.length == LENGTH) b.toString() else null
    }

    /** format shows a code to a person: "K7Q2-M9PX". */
    fun format(code: String): String = if (code.length == LENGTH) code.substring(0, 4) + "-" + code.substring(4) else code

    /** slot returns the slot of a code (32 hex) and its sealing key; null — not a code. */
    fun slot(code: String): Pair<String, ByteArray>? {
        val c = normalize(code) ?: return null
        val g = PKCS5S2ParametersGenerator(SHA256Digest())
        g.init(c.toByteArray(Charsets.US_ASCII), SALT.toByteArray(Charsets.US_ASCII), ROUNDS)
        val k = (g.generateDerivedParameters(48 * 8) as KeyParameter).key
        return k.copyOf(16).hex() to k.copyOfRange(16, 48)
    }

    private fun aead(forEncryption: Boolean, key: ByteArray, nonce: ByteArray, slot: String) =
        ChaCha20Poly1305().also { it.init(forEncryption, AEADParameters(KeyParameter(key), 128, nonce, unhex(slot))) }

    /** seal returns the slot and box of an invite under a code. */
    fun seal(code: String, invite: String): Pair<String, String> {
        val (slot, key) = slot(code) ?: throw IllegalArgumentException("not a code")
        val nonce = randomBytes(12)
        val pt = invite.toByteArray(Charsets.UTF_8)
        val c = aead(true, key, nonce, slot)
        val out = ByteArray(c.getOutputSize(pt.size))
        val n = c.processBytes(pt, 0, pt.size, out, 0)
        c.doFinal(out, n)
        return slot to Base64.getUrlEncoder().withoutPadding().encodeToString(nonce + out)
    }

    /** open opens a box with the code; null — it does not open, or the invite is corrupt or expired. */
    fun open(code: String, box: String): String? {
        val (slot, key) = slot(code) ?: return null
        return openBox(slot, key, box)
    }

    internal fun openBox(slot: String, key: ByteArray, box: String): String? {
        val raw = runCatching { Base64.getUrlDecoder().decode(box.trimEnd('=')) }.getOrNull() ?: return null
        if (raw.size < 12 + 16) return null
        val pt = runCatching {
            val c = aead(false, key, raw.copyOf(12), slot)
            val out = ByteArray(c.getOutputSize(raw.size - 12))
            val n = c.processBytes(raw, 12, raw.size - 12, out, 0)
            c.doFinal(out, n)
            String(out, Charsets.UTF_8)
        }.getOrNull() ?: return null
        return if (runCatching { Invite.parse(pt) }.isSuccess) pt else null
    }

    /** inviteFromTxt returns the invite of a `_p2pact._udp` record (TXT as key → value) that shows [code]; null — it does not. */
    fun inviteFromTxt(code: String, txt: Map<String, String>): String? {
        val (slot, key) = slot(code) ?: return null
        if (txt["t"] != slot) return null
        val pieces = txt.keys.mapNotNull { k -> if (k.startsWith("b")) k.substring(1).toIntOrNull()?.takeIf { it >= 0 } else null }.sorted()
        val box = StringBuilder()
        for ((i, n) in pieces.withIndex()) {
            if (n != i) return null // a piece is missing
            box.append(txt["b$n"])
        }
        return openBox(slot, key, box.toString())
    }

    /**
     * fetch gets the invite shown under [code] from a relay (spec §3, code slots); blocking.
     * Throws [CodeNotFoundException] — nobody shows it; [CodeUnsupportedException] — an older relay;
     * IOException — the relay is unreachable.
     */
    fun fetch(relay: RelayHint, code: String, timeoutMs: Int = 15_000): String {
        val (slot, key) = slot(code) ?: throw IllegalArgumentException("not a code")
        val ans = slotCall(relay, mapOf("relay" to 1, "op" to "get", "slot" to slot), timeoutMs)
        val box = ans["box"] as? String ?: throw java.io.IOException("relay: no box")
        return openBox(slot, key, box) ?: throw java.io.IOException("code box does not open")
    }

    internal fun slotCall(relay: RelayHint, req: Map<String, Any?>, timeoutMs: Int): Map<*, *> {
        dial(relay, timeoutMs).use { s ->
            s.soTimeout = timeoutMs
            s.outputStream.write((Json.write(req) + "\n").toByteArray(Charsets.UTF_8))
            s.outputStream.flush()
            val line = BufferedReader(InputStreamReader(s.inputStream, Charsets.UTF_8), 8 shl 10).readLine()
                ?: throw java.io.IOException("relay closed")
            val m = runCatching { Json.parse(line) as? Map<*, *> }.getOrNull() ?: throw java.io.IOException("relay: bad answer")
            when (val e = m["error"] as? String) {
                null -> return m
                "not found" -> throw CodeNotFoundException()
                "bad hello" -> throw CodeUnsupportedException()
                else -> throw java.io.IOException("relay: $e")
            }
        }
    }

    /** dial opens TLS to a relay, pinned by fingerprint when given (as [RelayRoad] does). */
    private fun dial(hint: RelayHint, timeoutMs: Int): SSLSocket {
        val (host, port) = hostPort(hint.addr) ?: throw IllegalArgumentException("bad relay address")
        val ctx = SSLContext.getInstance("TLS")
        val pinned = hint.fp.isNotEmpty()
        if (pinned) {
            val want = hint.fp.lowercase()
            val tm = object : X509TrustManager {
                override fun checkClientTrusted(chain: Array<out X509Certificate>?, authType: String?) =
                    throw java.security.cert.CertificateException("not a server")
                override fun checkServerTrusted(chain: Array<out X509Certificate>?, authType: String?) {
                    val leaf = chain?.firstOrNull() ?: throw java.security.cert.CertificateException("no certificate")
                    if (MessageDigest.getInstance("SHA-256").digest(leaf.encoded).hex() != want) {
                        throw java.security.cert.CertificateException("relay certificate does not match the fingerprint")
                    }
                }
                override fun getAcceptedIssuers(): Array<X509Certificate> = arrayOf()
            }
            ctx.init(null, arrayOf<TrustManager>(tm), random)
        } else {
            ctx.init(null, null, random)
        }
        val s = ctx.socketFactory.createSocket() as SSLSocket
        if (!pinned) s.sslParameters = s.sslParameters.also { it.endpointIdentificationAlgorithm = "HTTPS" }
        try {
            s.connect(InetSocketAddress(host, port), timeoutMs)
            s.soTimeout = timeoutMs
            s.startHandshake()
        } catch (e: Exception) {
            s.close()
            throw e
        }
        return s
    }
}
