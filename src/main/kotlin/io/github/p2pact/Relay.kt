package io.github.p2pact

import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.security.MessageDigest
import java.security.cert.X509Certificate
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLSocket
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/**
 * RelayRoad is the relay road (spec §3): one channel per pair, a packet is a base64 line over
 * TLS. The server is p2pact-go's relay package:
 *
 *   → {"relay":1,"channel":"<32 hex>","role":"a"}   ← {"ok":true}
 *   → "p" keepalive, answered with "P"; any other line is forwarded to the other side.
 */
class RelayRoad internal constructor(val hint: RelayHint, private val recv: Receiver) : Road {
    override val name = "relay:" + hint.addr
    private val channels = ConcurrentHashMap<String, Chan>()

    private inner class Chan(val channel: String, val role: String) {
        val out = LinkedBlockingQueue<ByteArray>(256)
        @Volatile var stopped = false
        @Volatile var socket: SSLSocket? = null

        fun stop() {
            stopped = true
            try { socket?.close() } catch (e: Exception) { }
        }
    }

    /** send puts a packet into a channel; a channel not joined is an error. */
    override fun send(addr: String, pkt: ByteArray): Boolean {
        val c = channels[addr] ?: return false
        return c.out.offer(pkt)
    }

    /** join keeps a channel open, reconnecting until leave. */
    internal fun join(channel: String, role: String) {
        if (channels.containsKey(channel)) return
        val c = Chan(channel, role)
        if (channels.putIfAbsent(channel, c) != null) return
        Thread({ keep(c) }, "p2pact-relay").apply { isDaemon = true }.start()
    }

    internal fun leave(channel: String) {
        channels.remove(channel)?.stop()
    }

    internal fun close() {
        for (k in channels.keys.toList()) leave(k)
    }

    /** open counts joined channels (tests). */
    internal val open: Int get() = channels.size

    private fun keep(c: Chan) {
        var backoff = 500L
        while (!c.stopped) {
            val ok = try { session(c) } catch (e: Exception) { false }
            if (c.stopped) return
            try { Thread.sleep(backoff) } catch (e: InterruptedException) { return }
            backoff = if (ok) 500L else minOf(backoff * 2, 30_000L)
        }
    }

    private fun dial(): SSLSocket {
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
                    // verified by fingerprint: the SHA-256 of the certificate
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
        s.connect(InetSocketAddress(host, port), 10_000)
        s.startHandshake()
        return s
    }

    /** session runs one connection; true — it was established. */
    private fun session(c: Chan): Boolean {
        val s = dial()
        c.socket = s
        if (c.stopped) { s.close(); return false }
        try {
            val os = s.outputStream
            os.write((Json.write(mapOf("relay" to 1, "channel" to c.channel, "role" to c.role)) + "\n").toByteArray())
            os.flush()
            s.soTimeout = 15_000
            val rd = BufferedReader(InputStreamReader(s.inputStream, Charsets.US_ASCII), 64 shl 10)
            val first = rd.readLine() ?: return false
            if (!first.contains("\"ok\":true")) return false
            val writer = Thread({
                try {
                    val enc = Base64.getEncoder()
                    while (!c.stopped && !s.isClosed) {
                        val pkt = c.out.poll(25, TimeUnit.SECONDS)
                        val line = if (pkt == null) "p\n" else enc.encodeToString(pkt) + "\n"
                        os.write(line.toByteArray(Charsets.US_ASCII))
                        os.flush()
                    }
                } catch (e: Exception) {
                } finally {
                    try { s.close() } catch (e: Exception) { }
                }
            }, "p2pact-relay-w").apply { isDaemon = true }
            writer.start()
            val ep = Endpoint(this, c.channel)
            val dec = Base64.getDecoder()
            s.soTimeout = 90_000
            while (!c.stopped) {
                val line = rd.readLine()?.trim() ?: break
                if (line.isEmpty() || line == "P") continue
                val pkt = try { dec.decode(line) } catch (e: IllegalArgumentException) { continue }
                if (pkt.size > MAX_PACKET + 64) continue
                recv(pkt, ep)
            }
        } finally {
            try { s.close() } catch (e: Exception) { }
        }
        return true
    }
}
