package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class ToolsTest : NetTestBase() {
    @Test
    fun validToolName() {
        val want = mapOf(
            "com.example.jira" to true, "org.a-b.c1" to true, "jira" to false, "" to false,
            "Com.example" to false, "com..x" to false, "p2pact.chat" to false, "com.-x" to false,
        )
        for ((name, ok) in want) assertEquals(name, ok, validToolName(name))
    }

    // A name gets a number of the device's own, from 64 up, not one a tool already has; the same
    // name on another device may have another number, and callers resolve it by name.
    @Test
    fun namedToolsNumberedPerDevice() {
        val a = node("Mac")
        val b = node("Phone") // both have the echo at 64
        fun reg(n: Node, name: String) {
            val num = n.registerTool(name, 3)
            assertTrue("$name: $num", num >= FIRST_NAMED_TOOL && num != 64)
            assertEquals(num, n.registerTool(name, 3))
            n.handleTool(name, 1) { _, _ -> listOf(n.name + ":" + name) }
        }
        reg(a, "com.acme.one"); reg(a, "com.acme.two")
        reg(b, "com.acme.two"); reg(b, "com.acme.one")
        try { a.registerTool("jira", 1); fail("a bad name was taken") } catch (e: IllegalArgumentException) { }
        try { a.registerTool("p2pact.chat", 1); fail("a name of the spec was taken") } catch (e: IllegalArgumentException) { }
        pair(a, b)
        eventually("hellos") {
            a.toolNumber(b.id.addr, "com.acme.one") != null && b.toolNumber(a.id.addr, "com.acme.one") != null
        }
        val na = a.toolNumber(b.id.addr, "com.acme.one")!! // b's number
        val nb = b.toolNumber(a.id.addr, "com.acme.one")!! // a's number
        assertNotEquals("the numbers should differ per device", na, nb)
        assertEquals(1, a.toolNumber(b.id.addr, "p2pact.chat"))
        for ((from, to, name) in listOf(Triple(a, b, "com.acme.one"), Triple(a, b, "com.acme.two"), Triple(b, a, "com.acme.one"))) {
            val res = from.callTool(to.id.addr, name, 1, timeoutMs = 10_000).get()
            assertEquals(listOf<Any?>(to.name + ":" + name), res)
        }
        assertEquals("com.acme.one", a.peerToolName(b.id.addr, na))
        assertEquals("com.acme.one", a.toolName(nb))
        assertEquals("p2pact.chat", a.toolName(1))
        assertEquals(Errors.NO_METHOD, code(a.callTool(b.id.addr, "com.acme.none", 1)))
        // a tool registered after the session is up reaches the other side at once
        a.registerTool("com.acme.late", 1)
        eventually("late tool") { b.toolNumber(a.id.addr, "com.acme.late") != null }
    }

    // A hello without the named map (an older device) names nothing; its numbered tools work as before.
    @Test
    fun oldHelloNamesNothing() {
        val a = node("Mac")
        val b = node("Old")
        pair(a, b)
        val hello = Cbor.encode(listOf(3, 0, 2, "Old", "linux", "old-impl", mapOf(64 to 1), emptyList<ByteArray>(), Hints().toWire()))
        a.deliverForTest(b.id.addr, hello)
        assertNull(a.toolNumber(b.id.addr, "com.acme.one"))
        assertFalse(a.trust.get(b.id.addr) == null)
        echo(a, b, "numbered".toByteArray())
    }
}
