package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

private const val ID = "0b7c2e64-1d2a-4f5e-9a10-6c3b8d7e9f01"

class FleetClientTest : ClientTestBase() {
    @Test
    fun readMethods() {
        val fleet = connect().fleet
        val hello = fleet.hello().await()
        assertEquals("0.2", hello.protocol)
        assertEquals("MacBook Pro", hello.node.name)
        assertEquals("darwin", hello.node.os)
        assertEquals("claude", hello.node.agents.single().id)
        assertEquals(1L, hello.node.status["working"])
        assertEquals("MacBook Pro", fleet.node().await().name)

        val s = fleet.sessions().await().single()
        assertEquals(ID, s.id)
        assertEquals("claude", s.agent)
        assertEquals("/Users/me/app", s.dir)
        assertEquals("working", s.phase)
        assertEquals("Починить вход", s.title)
        assertEquals("2026-10-27T10:00:00Z", s.updated)
        assertEquals("3f2a9c01d4e5b6a7", s.raw.obj("status")!!.obj("owner")!!.str("node"))
        assertEquals(ID, fleet.session(ID).await().id)
        assertSent(2, "session/get", "p2pact.fleet", "session-get.request")

        val m = fleet.messages(ID, after = 2).await()
        assertSent(2, "session/messages", "p2pact.fleet", "session-messages.request")
        assertEquals(3L, m.last)
        assertTrue(m.older)
        assertEquals("Готово", m.messages.single().text)
        assertEquals("assistant", m.messages.single().role)
    }

    @Test
    fun watchDeliversEventsInOrder() {
        val fleet = connect().fleet
        val events = fleet.events.queue()
        val watch = fleet.watch(false).await()
        assertSent(2, "fleet/watch", "p2pact.fleet", "watch.request")
        assertEquals(ID, watch.sessions.single().id)

        for (n in listOf("notify-session-changed", "notify-session-messages", "notify-notify")) {
            engine.notify(2, Fixtures.read("p2pact.fleet", n))
        }
        val changed = events.next() as FleetEvent.SessionsChanged
        assertEquals("working", changed.sessions.single().phase)
        val msgs = events.next() as FleetEvent.SessionMessages
        assertEquals(ID, msgs.id)
        assertEquals(4L, msgs.messages.single().seq)
        assertEquals("Продолжай", msgs.messages.single().text)
        assertEquals("Агент ждёт ответа", (events.next() as FleetEvent.Notify).text)

        engine.notify(2, mapOf("jsonrpc" to "2.0", "method" to "session/resync", "params" to mapOf("id" to ID)))
        assertEquals(ID, (events.next() as FleetEvent.Resync).id)
        engine.notify(2, mapOf("jsonrpc" to "2.0", "method" to "something/new", "params" to mapOf("k" to 1)))
        assertEquals("something/new", (events.next() as FleetEvent.Other).method)

        watch.close()
        // the unwatch reaches the engine (the call is not awaited by close)
        for (i in 0..500) { if (engine.seen.any { it.method == "fleet/unwatch" }) break; Thread.sleep(10) }
        assertTrue(engine.seen.any { it.method == "fleet/unwatch" })
    }

    @Test
    fun writeMethods() {
        val fleet = connect().fleet
        val s = fleet.create(SessionSpec(agent = "claude", dir = "/Users/me/app", perms = "ask"), "Почини вход").await()
        assertSent(2, "session/create", "p2pact.fleet", "session-create.request")
        assertEquals("starting", s.phase)

        fleet.send(ID, "Продолжай").await()
        assertSent(2, "session/send", "p2pact.fleet", "session-send.request")

        val e = try { fleet.send("нет", "?").await(); null } catch (e: Throwable) { e } as RpcException
        assertTrue(e.isNoSuchResource)
        assertEquals(-32004, e.code)

        assertEquals(mapOf("sleep_after" to 15L), fleet.setSettings(mapOf("sleep_after" to 15)).await())
        assertSent(2, "settings/set", "p2pact.fleet", "settings-set.request")

        fleet.answer(ID, waitId = "w1", answers = mapOf("Q" to "A")).await()
        assertEquals(mapOf("id" to ID, "waitId" to "w1", "answers" to mapOf("Q" to "A")), engine.seen.last { it.method == "session/answer" }.params)
        fleet.interrupt(ID).await()
        assertFalse(fleet.close(ID).await().warning != null)
        fleet.setKeep(ID, true).await()
        assertEquals(mapOf("id" to ID, "on" to true), engine.seen.last { it.method == "session/setKeep" }.params)
    }

    @Test
    fun readOnlyFleet() {
        val computer = connect(decide = { listOf(Grant("p2pact.fleet", LEVEL_READ)) })
        val fleet = computer.fleet
        assertEquals(1, fleet.sessions().await().size)
        for (write in listOf({ fleet.send(ID, "x") }, { fleet.create(SessionSpec(agent = "claude")) }, { fleet.interrupt(ID) })) {
            val e = try { write().await(); null } catch (e: Throwable) { e }
            assertTrue("got $e", e is ForbiddenException)
        }
        // no grant on the chat at all: even reading is refused
        val c = try { computer.chat.listSessions().await(); null } catch (e: Throwable) { e }
        assertTrue("got $c", c is ForbiddenException)
    }
}
