package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import io.github.p2pact.ToolHandler
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/**
 * The terminal by its spec name (tool 5, core spec §8.1 "Former names"): a computer that serves it
 * only under a former name is called by that name once the alias is declared, and a grant on the
 * former name counts; when the computer serves p2pact.term too, the call goes to tool 5.
 */
class TermClientTest : ClientTestBase() {
    private val former = "com.example.oldterm"

    private fun answer(who: String) = ToolHandler { _, _, _, _ -> listOf(who) }

    private fun waitFor(what: String, cond: () -> Boolean) {
        for (i in 0..500) {
            if (cond()) return
            Thread.sleep(10)
        }
        throw AssertionError("timeout: $what")
    }

    @Test
    fun termByAliasThenByNumber() {
        val computer = connect(DEFAULT_WANTS + Grant(former, LEVEL_WRITE))
        client.node.aliasTool(former, Tools.TERM)
        engine.node.aliasTool(former, Tools.TERM)
        engine.node.handleToolAll(former, answer("former"))
        assertEquals(LEVEL_WRITE, computer.level(Tools.TERM))
        assertTrue(computer.canWrite(former))
        waitFor("former name in the hello") { client.node.peerHasTool(computer.id, former) }
        assertFalse(client.node.peerHasTool(computer.id, Tools.TERM))
        assertEquals(listOf<Any?>("former"), computer.callTool(Tools.TERM, 1).await())

        engine.node.handleToolAll(Tools.TERM, answer("term"))
        waitFor("tool 5 in the hello") { client.node.peerHasTool(computer.id, Tools.TERM) }
        assertEquals(listOf<Any?>("term"), computer.callTool(Tools.TERM, 1).await())
    }

    @Test
    fun readOnFormerNameIsNotWrite() {
        val computer = connect(DEFAULT_WANTS + Grant(former, LEVEL_READ))
        client.node.aliasTool(former, Tools.TERM)
        engine.node.aliasTool(former, Tools.TERM)
        engine.node.handleToolAll(Tools.TERM) { _, level, method, _ ->
            if (method != 1 && level < LEVEL_WRITE) throw io.github.p2pact.P2pactError(io.github.p2pact.Errors.FORBIDDEN, "read only")
            listOf("term")
        }
        waitFor("tool 5 in the hello") { client.node.peerHasTool(computer.id, Tools.TERM) }
        assertEquals(LEVEL_READ, computer.level(Tools.TERM))
        assertTrue(computer.canRead(Tools.TERM))
        assertFalse(computer.canWrite(Tools.TERM))
        assertEquals(listOf<Any?>("term"), computer.callTool(Tools.TERM, 1).await())
        val e = try { computer.callTool(Tools.TERM, 2).await(); null } catch (e: Throwable) { e }
        assertTrue("got $e", e is ForbiddenException)
    }

    @Test
    fun eventsOfTool5AreNamedTerm() {
        val computer = connect(DEFAULT_WANTS + Grant(Tools.TERM, LEVEL_READ))
        val events = client.toolEvents.queue()
        engine.node.eventOn(engine.clientAddr, 0, 5, 1, "hi")
        val e = events.next()
        assertEquals(computer.id, e.computer.id)
        assertEquals(Tools.TERM, e.tool)
        assertEquals(5, e.toolNumber)
        assertEquals(listOf<Any?>("hi"), e.args)
    }
}
