package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.DatagramPacket
import java.net.DatagramSocket
import java.net.InetAddress
import java.net.SocketTimeoutException

/** The NAT prime (spec §7): one byte to port 3478 of a public host before a datagram to it, after primeIdle without one. */
class PrimeTest {
    @Test
    fun publicAddresses() {
        val cases = mapOf(
            "178.88.48.86" to true, "161.35.71.42" to true, "::ffff:8.8.8.8" to true,
            "10.1.2.3" to false, "192.168.31.178" to false, "172.16.0.1" to false, "100.94.117.6" to false,
            "127.0.0.1" to false, "169.254.1.1" to false, "2001:db8::1" to false, "0.0.0.0" to false, "240.0.0.1" to false,
        )
        for ((ip, public) in cases) assertEquals(ip, public, natPublic(InetAddress.getByName(ip)))
    }

    @Test
    fun primesFirstAndAfterIdle() {
        val stun = DatagramSocket(0, InetAddress.getByName("127.0.0.1"))
        val peer = DatagramSocket(0, InetAddress.getByName("127.0.0.1"))
        val old = listOf(primeIdle, primeGap, primePort.toLong())
        val oldHost = primeHost
        primeIdle = 600 * MS
        primeGap = 150 * MS
        primePort = stun.localPort
        primeHost = { true }
        val u = UdpRoad("127.0.0.1:0") { _, _ -> }
        val gapMs = primeGap / MS
        try {
            fun primes(): Int {
                var n = 0
                val buf = ByteArray(64)
                stun.soTimeout = 50
                while (true) {
                    val dp = DatagramPacket(buf, buf.size)
                    try { stun.receive(dp) } catch (e: SocketTimeoutException) { return n }
                    assertEquals(1, dp.length)
                    assertEquals(u.localAddr.substringAfterLast(':').toInt(), dp.port)
                    n++
                }
            }
            // got reads what the peer received, each with how many ms after start it came
            fun got(want: Int, start: Long): Map<String, Long> {
                val out = HashMap<String, Long>()
                val buf = ByteArray(64)
                peer.soTimeout = 1000
                while (out.size < want) {
                    val dp = DatagramPacket(buf, buf.size)
                    peer.receive(dp)
                    out[String(buf, 0, dp.length)] = (System.nanoTime() - start) / 1_000_000
                }
                return out
            }
            val to = "127.0.0.1:${peer.localPort}"

            var start = System.nanoTime()
            u.send(to, "first".toByteArray())
            u.send(to, "second".toByteArray())
            assertEquals("first datagrams", 1, primes())
            for ((m, at) in got(2, start)) assertTrue("$m came ${at} ms after the prime", at >= gapMs - 10)

            start = System.nanoTime()
            u.send(to, "third".toByteArray())
            assertTrue("a datagram after the gap waited", got(1, start)["third"]!! < gapMs - 10)
            assertEquals("a datagram right after", 0, primes())

            Thread.sleep(650)
            u.send(to, "after idle".toByteArray())
            got(1, System.nanoTime())
            assertEquals("after primeIdle", 1, primes())

            primeHost = { false }
            Thread.sleep(650)
            start = System.nanoTime()
            u.send(to, "private".toByteArray())
            assertTrue("a datagram to a private address waited", got(1, start)["private"]!! < gapMs - 10)
            assertEquals("a private address", 0, primes())
        } finally {
            u.close()
            stun.close()
            peer.close()
            primeIdle = old[0]
            primeGap = old[1]
            primePort = old[2].toInt()
            primeHost = oldHost
        }
    }
}
