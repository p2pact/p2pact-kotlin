package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assume.assumeTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

/**
 * Two devices that reach each other only through a relay (p2pact-go's cmd/p2pact-relay). Run with
 * P2PACT_RELAY="<host:port> <certificate fingerprint>"; skipped otherwise.
 */
class RelayTest {
    @get:Rule
    val tmp = TemporaryFolder()
    private val nodes = ArrayList<Node>()

    @After
    fun closeAll() = nodes.forEach { it.close() }

    @Test
    fun pairAndTalkThroughTheRelay() {
        val env = System.getenv("P2PACT_RELAY")
        assumeTrue("P2PACT_RELAY not set", !env.isNullOrBlank())
        val (addr, fp) = env!!.trim().split(Regex("\\s+"))
        val hint = RelayHint(addr, fp)
        fun node(name: String) = Node(tmp.newFolder(), name).also {
            it.setRelay(hint)
            it.handle(64, 1) { _, args -> listOf(args[0]) }
            it.start()
            nodes.add(it)
        }
        val a = node("a")
        val b = node("b")
        val p = b.join(Invite.parse(a.invite(60_000).toString()), 20_000)
        assertEquals(a.id.addr, p.addr)
        val data = ByteArray(50_000) { (it % 13).toByte() }
        assertArrayEquals(data, b.call(a.id.addr, 64, 1, data, timeoutMs = 30_000).get()[0] as ByteArray)
        for (i in 0 until 50) if (a.trust.get(b.id.addr) == null) Thread.sleep(50)
        assertArrayEquals(data, a.callOn(b.id.addr, 1, 64, 1, data, timeoutMs = 30_000).get()[0] as ByteArray)
        assertEquals(listOf("relay:$addr"), a.paths(b.id.addr))
    }
}
