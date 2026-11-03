package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.File
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class GrantsTest : NetTestBase() {
    @Test
    fun levelRules() {
        val g = listOf(Grant("com.a.x", LEVEL_READ), Grant("com.a.y", LEVEL_WRITE))
        for ((tool, want) in mapOf("com.a.x" to 1, "com.a.y" to 2, "com.a.z" to 0, "" to 0)) {
            assertEquals(tool, want, grantLevel(g, tool))
        }
        assertEquals(LEVEL_WRITE, grantLevel(null, "any"))
        assertEquals(0, grantLevel(emptyList(), "any"))
        assertEquals(LEVEL_WRITE, grantLevel(listOf(Grant(ALL_TOOLS, LEVEL_READ), Grant("com.a.x", LEVEL_WRITE)), "com.a.x"))
    }

    // Devices whose grants were never set (every device paired before grants) use everything.
    @Test
    fun defaultsWhenNeverSet() {
        val dir = tmp.newFolder()
        val a = node("Mac", dir = dir)
        val b = node("Phone")
        pair(a, b)
        assertEquals(defaultGrants(), a.grants(b.id.addr))
        echo(b, a, "as before".toByteArray())
        // a trust file from before grants has no such field
        a.trust.flush()
        val file = File(dir, "net-peers.json")
        val old = (Json.parse(file.readText()) as List<*>).map { (it as Map<*, *>).filterKeys { k -> k != "grants" && k != "granted" } }
        val dir2 = tmp.newFolder()
        File(dir2, "net-peers.json").writeText(Json.write(old))
        val p = Trust(dir2).get(b.id.addr)
        assertNotNull(p)
        assertNull(p!!.grants)
        assertEquals(defaultGrants(), p.allowed())
    }

    @Test
    fun persist() {
        val dir = tmp.newFolder()
        val a = node("Mac", dir = dir)
        val b = node("Phone")
        pair(a, b)
        val want = listOf(Grant("com.a.x", LEVEL_READ))
        a.setGrants(b.id.addr, want)
        a.trust.flush()
        assertEquals(want, Trust(dir).get(b.id.addr)!!.allowed())
        a.revokeGrants(b.id.addr)
        a.trust.flush()
        val p = Trust(dir).get(b.id.addr)!!
        assertNotNull("revoked grants come back as the default", p.grants)
        assertEquals(0, p.allowed().size)
        try { a.setGrants("nobody", want); fail("grants for a stranger") } catch (e: IllegalArgumentException) { }
    }

    // A call outside the grants is error 4; read methods need read, the rest write; tool 0 is always allowed.
    @Test
    fun enforced() {
        val a = node("Mac")
        val b = node("Phone")
        val ok = Handler { _, _ -> listOf("ok") }
        a.handleTool("com.acme.notes", 1, ok) // write
        a.handleTool("com.acme.notes", 2, ok) // read
        a.handleTool("com.acme.mail", 1, ok)
        a.markRead(a.registerTool("com.acme.notes"), 2)
        pair(a, b)
        val addr = a.id.addr
        val me = b.id.addr
        fun call(tool: String, method: Int) = code(b.callTool(addr, tool, method, timeoutMs = 10_000))
        eventually("hello") { b.toolNumber(addr, "com.acme.notes") != null }
        assertTrue("the default allows everything", call("com.acme.notes", 1) == 0 && call("com.acme.mail", 1) == 0)
        a.setGrants(me, listOf(Grant("com.acme.notes", LEVEL_READ)))
        assertEquals("read method at read", 0, call("com.acme.notes", 2))
        assertEquals("write method at read", Errors.FORBIDDEN, call("com.acme.notes", 1))
        assertEquals("a tool outside the grants", Errors.FORBIDDEN, call("com.acme.mail", 1))
        assertEquals("an unnamed tool without *", Errors.FORBIDDEN, code(b.call(addr, 64, 1, ByteArray(1), timeoutMs = 10_000))) // the echo has no name
        assertEquals("ping", 0, code(b.call(addr, 0, 1, timeoutMs = 10_000)))
        a.grant(me, "com.acme.notes", LEVEL_WRITE)
        assertEquals("write after widening", 0, call("com.acme.notes", 1))
        a.revokeGrants(me)
        assertEquals("after revoking", Errors.FORBIDDEN, call("com.acme.notes", 2))
        a.setGrants(me, listOf(Grant(ALL_TOOLS, LEVEL_READ)))
        assertEquals(Errors.FORBIDDEN, call("com.acme.mail", 1))
        assertEquals(0, call("com.acme.notes", 2))
    }

    // A tool whose one method carries many checks the level itself.
    @Test
    fun levelHandler() {
        val a = node("Mac")
        val b = node("Phone")
        a.handleLevel(1, 1) { _, level, args ->
            val m = args[0] as String
            if (m != "session/list" && level < LEVEL_WRITE) throw P2pactError(Errors.FORBIDDEN, "read only")
            listOf(m)
        }
        pair(a, b)
        val addr = a.id.addr
        fun call(m: String) = code(b.call(addr, 1, 1, m, timeoutMs = 10_000))
        a.setGrants(b.id.addr, listOf(Grant("p2pact.chat", LEVEL_READ)))
        assertEquals("read", 0, call("session/list"))
        assertEquals("write at read", Errors.FORBIDDEN, call("session/prompt"))
        a.setGrants(b.id.addr, listOf(Grant("p2pact.chat", LEVEL_WRITE)))
        assertEquals("write", 0, call("session/prompt"))
        a.setGrants(b.id.addr, emptyList())
        assertEquals("no grant", Errors.FORBIDDEN, call("session/list"))
    }

    // Events of a tool go only to devices with at least read on it; the device is told its grants (0.6).
    @Test
    fun eventsAndTell() {
        val a = node("Mac")
        val b = node("Phone")
        val events = LinkedBlockingQueue<String>()
        val told = LinkedBlockingQueue<List<Grant>>()
        b.onEvent = { _, _, _, args -> events.add(args[0] as String) }
        b.onGranted = { _, g -> told.add(g) }
        val num = a.registerTool("com.acme.feed", 1)
        pair(a, b)
        val addr = a.id.addr
        val me = b.id.addr
        fun expect(want: String) = assertEquals(want, events.poll(3, TimeUnit.SECONDS))
        a.event(me, num, 1, "everyone")
        expect("everyone")
        a.setGrants(me, listOf(Grant("com.acme.other", LEVEL_WRITE)))
        assertEquals(listOf(Grant("com.acme.other", LEVEL_WRITE)), told.poll(3, TimeUnit.SECONDS))
        assertEquals(listOf(Grant("com.acme.other", LEVEL_WRITE)), b.grantedBy(addr))
        a.event(me, num, 1, "hidden") // no read: not sent
        Thread.sleep(300)
        a.grant(me, "com.acme.feed", LEVEL_READ)
        a.event(me, num, 1, "visible")
        expect("visible") // "hidden" never came
        // method 0.6: what the caller may use
        val res = b.call(addr, 0, 6, timeoutMs = 10_000).get()
        assertEquals(1, res.size)
        assertEquals(listOf(Grant("com.acme.other", LEVEL_WRITE), Grant("com.acme.feed", LEVEL_READ)), parseGrants(res[0]))
    }

    // Default grants send no event 0.6 (an older peer assumes them); a non-default set does on a new session.
    @Test
    fun noEventForDefault() {
        val a = node("Mac")
        val b = node("Phone")
        val told = LinkedBlockingQueue<List<Grant>>()
        b.onGranted = { _, g -> told.add(g) }
        pair(a, b)
        a.sendInfoForTest(b.id.addr)
        Thread.sleep(400)
        assertTrue("no 0.6 for the default", told.isEmpty())
        assertEquals(defaultGrants(), b.grantedBy(a.id.addr))
        assertNull(b.trust.get(a.id.addr)!!.granted)
    }
}
