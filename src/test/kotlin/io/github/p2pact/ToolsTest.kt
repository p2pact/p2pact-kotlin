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

    // One handler for every method of a named tool (a device hosting another program's tool): it gets
    // the method and the caller's level; the hello announces the tool; unregisterTool takes it back.
    @Test
    fun toolWideHandler() {
        val host = node("Host")
        val client = node("Client")
        host.handleToolAll("com.acme.hosted") { _, level, method, args ->
            if (method == 2 && level < LEVEL_WRITE) throw P2pactError(Errors.FORBIDDEN, "forbidden")
            listOf("m$method:l$level:" + args[0])
        }
        pair(host, client)
        eventually("hello") { client.peerTools(host.id.addr).contains("com.acme.hosted") }
        host.setGrants(client.id.addr, listOf(Grant("com.acme.hosted", LEVEL_READ)))
        assertEquals(listOf<Any?>("m7:l1:x"), client.callTool(host.id.addr, "com.acme.hosted", 7, "x", timeoutMs = 10_000).get())
        assertEquals(Errors.FORBIDDEN, code(client.callTool(host.id.addr, "com.acme.hosted", 2, "x", timeoutMs = 10_000)))
        host.setGrants(client.id.addr, listOf(Grant("com.acme.hosted", LEVEL_WRITE)))
        assertEquals(listOf<Any?>("m2:l2:x"), client.callTool(host.id.addr, "com.acme.hosted", 2, "x", timeoutMs = 10_000).get())
        host.unregisterTool("com.acme.hosted")
        eventually("tool gone") { client.peerTools(host.id.addr).isEmpty() }
    }

    // Tool 5 p2pact.term is a tool of the spec: not a named one, served by the application under its fixed
    // number with handleToolAll, announced in {tool: version}, read lets in the read methods only.
    @Test
    fun fixedToolServedByApp() {
        val a = node("Mac")
        val b = node("Phone")
        assertEquals(5, fixedTool("p2pact.term"))
        assertNull(fixedTool("com.acme.one"))
        try { a.registerTool("p2pact.term", 1); fail("a name of the spec was registered as a named tool") } catch (e: IllegalArgumentException) { }
        pair(a, b)
        val addr = a.id.addr
        val me = b.id.addr
        a.handleToolAll("p2pact.term") { _, level, method, _ ->
            if (method != 1 && level < LEVEL_WRITE) throw P2pactError(Errors.FORBIDDEN, "read only")
            listOf("term", method.toLong())
        }
        eventually("term in the hello") { b.peerHasTool(addr, "p2pact.term") }
        assertTrue("a tool of the spec went into the named map", b.peerTools(addr).isEmpty())
        assertEquals(Node.ResolvedTool(5, "p2pact.term"), b.resolveTool(addr, "p2pact.term"))
        assertEquals("p2pact.term", b.peerToolName(addr, 5))
        fun call(m: Int) = code(b.callTool(addr, "p2pact.term", m))
        assertEquals(0, call(1))
        a.setGrants(me, listOf(Grant("p2pact.term", LEVEL_READ)))
        assertEquals("read", 0, call(1))
        assertEquals("write at read", Errors.FORBIDDEN, call(2))
        a.setGrants(me, listOf(Grant("p2pact.chat", LEVEL_WRITE)))
        assertEquals("no grant on term", Errors.FORBIDDEN, call(1))
        a.unregisterTool("p2pact.term")
        eventually("term gone from the hello") { !b.peerHasTool(addr, "p2pact.term") }
    }

    // An alias (spec §8.1): one tool under the spec's name and a former one. Grants on either name
    // count for both, the highest level wins, for calls and for events; each name answers under its own number.
    @Test
    fun aliasRights() {
        val a = node("Mac")
        val b = node("Phone")
        try { a.aliasTool("p2pact.chat", "p2pact.term"); fail("a name of the spec taken as an alias") } catch (e: IllegalArgumentException) { }
        try { a.aliasTool("com.example.term", "com.example.term"); fail("a tool its own alias") } catch (e: IllegalArgumentException) { }
        a.aliasTool("com.example.term", "p2pact.term")
        fun served(name: String) = ToolHandler { _, level, method, _ ->
            if (method != 1 && level < LEVEL_WRITE) throw P2pactError(Errors.FORBIDDEN, "read only")
            listOf(name)
        }
        a.handleToolAll("p2pact.term", served("p2pact.term"))
        a.handleToolAll("com.example.term", served("com.example.term"))
        val events = java.util.concurrent.LinkedBlockingQueue<Int>()
        b.onEvent = { _, tool, _, _ -> events.add(tool) }
        pair(a, b)
        val addr = a.id.addr
        val me = b.id.addr
        eventually("both names") { b.peerHasTool(addr, "p2pact.term") && b.peerHasTool(addr, "com.example.term") }
        val alias = b.toolNumber(addr, "com.example.term")!!
        fun call(name: String, m: Int) = code(b.callTool(addr, name, m))
        val cases = listOf(
            listOf(Grant("com.example.term", LEVEL_WRITE)) to LEVEL_WRITE,
            listOf(Grant("p2pact.term", LEVEL_WRITE)) to LEVEL_WRITE,
            listOf(Grant("p2pact.term", LEVEL_READ), Grant("com.example.term", LEVEL_WRITE)) to LEVEL_WRITE,
            listOf(Grant("com.example.term", LEVEL_READ)) to LEVEL_READ,
            listOf(Grant("p2pact.term", LEVEL_READ)) to LEVEL_READ,
            listOf(Grant("p2pact.chat", LEVEL_WRITE)) to 0,
        )
        for ((grants, level) in cases) {
            a.setGrants(me, grants)
            for (name in listOf("p2pact.term", "com.example.term")) {
                assertEquals("$grants: level on $name", level, a.level(me, name))
                assertEquals("$grants: read on $name", level >= LEVEL_READ, call(name, 1) == 0)
                assertEquals("$grants: write on $name", level >= LEVEL_WRITE, call(name, 2) == 0)
            }
            // events of either number reach a device with read on either name
            events.clear()
            a.event(me, 5, 3)
            a.event(me, alias, 3)
            val got = ArrayList<Int>()
            while (got.size < 2) got.add(events.poll(1, java.util.concurrent.TimeUnit.SECONDS) ?: break)
            if (level >= LEVEL_READ) assertEquals("$grants: events", listOf(5, alias), got) else assertTrue("$grants: events $got", got.isEmpty())
        }
    }

    // A device that knows the terminal only by its former name and one that serves p2pact.term talk
    // both ways: a new caller reaches the old device by the alias, an old caller reaches the new device
    // by the former name with grants on p2pact.term, a new one goes to 5.
    @Test
    fun aliasOldAndNewDevices() {
        val old = node("Old")
        val nw = node("New")
        val phone = node("Phone")
        fun answer(who: String) = ToolHandler { _, _, _, _ -> listOf(who) }
        old.handleToolAll("com.example.term", answer("old")) // before p2pact.term
        nw.aliasTool("com.example.term", "p2pact.term")
        nw.handleToolAll("p2pact.term", answer("new:5"))
        nw.handleToolAll("com.example.term", answer("new:alias"))
        phone.aliasTool("com.example.term", "p2pact.term") // a new caller
        pair(nw, old)
        pair(phone, old)
        pair(phone, nw)
        val oldAddr = old.id.addr
        val nwAddr = nw.id.addr
        eventually("hellos") {
            phone.peerHasTool(oldAddr, "com.example.term") && phone.peerHasTool(nwAddr, "p2pact.term") &&
                old.peerHasTool(nwAddr, "com.example.term") && nw.peerHasTool(oldAddr, "com.example.term")
        }
        assertFalse("the old device announced 5", phone.peerHasTool(oldAddr, "p2pact.term"))
        phone.resolveTool(oldAddr, "p2pact.term")!!.let { assertTrue(it.number >= FIRST_NAMED_TOOL); assertEquals("com.example.term", it.name) }
        assertEquals(Node.ResolvedTool(5, "p2pact.term"), phone.resolveTool(nwAddr, "p2pact.term"))
        phone.resolveTool(nwAddr, "com.example.term")!!.let { assertTrue(it.number >= FIRST_NAMED_TOOL); assertEquals("com.example.term", it.name) }
        nw.setGrants(oldAddr, listOf(Grant("p2pact.term", LEVEL_WRITE))) // an old caller, granted the new name
        data class C(val from: Node, val to: String, val name: String, val want: String)
        for (c in listOf(
            C(phone, oldAddr, "p2pact.term", "old"),
            C(phone, nwAddr, "p2pact.term", "new:5"),
            C(nw, oldAddr, "p2pact.term", "old"),
            C(old, nwAddr, "com.example.term", "new:alias"),
        )) {
            assertEquals("${c.from.name} -> ${c.to} ${c.name}", listOf<Any?>(c.want), c.from.callTool(c.to, c.name, 1, timeoutMs = 10_000).get())
        }
        // a tool nobody announced is unknown, an unannounced tool of the spec goes by its number and the device refuses
        assertEquals(Errors.NO_METHOD, code(old.callTool(phone.id.addr, "com.example.none", 1)))
    }
}
