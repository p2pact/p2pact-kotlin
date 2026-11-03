package io.github.p2pact.client

import io.github.p2pact.Invite
import io.github.p2pact.Node
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** The client over a node the application runs itself: it owns the pairing and the callbacks, the SDK serves the chat. */
class WrappedNodeTest {
    @get:Rule
    val tmp = TemporaryFolder()
    private val closers = ArrayList<AutoCloseable>()

    @After
    fun closeAll() = closers.reversed().forEach { it.close() }

    @Test
    fun chatOverTheApplicationsNode() {
        val engine = FakeEngine(tmp.newFolder()).also { closers.add(it) }
        val node = Node(tmp.newFolder(), "Phone app")
        closers.add(AutoCloseable { node.close() })
        node.listenUdp("127.0.0.1:0")
        val client = P2pactClient(node)
        // the application's own callbacks, passing on to the SDK
        val hellos = LinkedBlockingQueue<String>()
        node.onHello = { addr -> client.hello(addr); hellos.add(addr) }
        node.onEvent = { addr, tool, ev, args -> client.event(addr, tool, ev, args) }
        node.start()

        val peer = node.join(Invite.parse(engine.invite()), 15_000)
        assertEquals(peer.addr, hellos.poll(10, TimeUnit.SECONDS))
        val computer = client.computer(peer.addr)
        assertNotNull(computer)
        assertEquals(ConnectionState.ONLINE, computer!!.state)

        val list = computer.chat.listSessions().await()
        assertEquals("s1", list.sessions.single().id)
        // a request on a stream of the application's choice (0: the computer's own)
        computer.chat.request("session/list", mapOf("sessionId" to "s1"), 0).await()
        assertEquals(listOf("initialize", "session/list", "session/list"), engine.seen.map { it.method })

        // closing the client leaves the application's node alone
        client.close()
        assertTrue(node.paths(peer.addr).isNotEmpty())
    }
}
