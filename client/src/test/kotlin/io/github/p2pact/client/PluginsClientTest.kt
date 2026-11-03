package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PluginsClientTest : ClientTestBase() {
    private val wants = DEFAULT_WANTS + Grant("com.example.hello", LEVEL_WRITE)

    @Test
    fun listLogGrantsEnableDisable() {
        val plugins = connect(wants).plugins
        val p = plugins.list().await().single()
        assertEquals("com.example.hello", p.name)
        assertEquals(PluginStatus.RUNNING, p.status)
        assertTrue(p.enabled)
        assertEquals(4242, p.pid)
        assertEquals(listOf("com.example.hello"), p.announced)
        assertEquals(listOf("p2pact.chat:read"), p.needs)

        assertEquals("started\nready\n", plugins.log("com.example.hello", 2).await())
        assertSent(4, "plugins/log", "p2pact.plugins", "plugins.log.request")

        val g = plugins.grants("com.example.hello").await()
        assertEquals(listOf(Grant("p2pact.chat", LEVEL_READ)), g.grants)
        assertEquals(mapOf("com.example.hello" to listOf(2)), g.read)
        // the engine sends the level as a string
        assertEquals(LEVEL_READ, g.grants.single().level)

        // plugins are asked for with read only: writing needs the person to widen it
        val e = try { plugins.enable("com.example.hello").await(); null } catch (e: Throwable) { e }
        assertTrue("got $e", e is ForbiddenException)
    }

    @Test
    fun levelAcceptsStringAndInteger() {
        assertEquals(LEVEL_READ, levelOf("read"))
        assertEquals(LEVEL_WRITE, levelOf("write"))
        assertEquals(LEVEL_READ, levelOf(1))
        assertEquals(LEVEL_WRITE, levelOf(2L))
        assertEquals(null, levelOf("nope"))
    }

    @Test
    fun unknownPluginLog() {
        val e = try { connect(wants).plugins.log("com.example.nope").await(); null } catch (e: Throwable) { e } as RpcException
        assertEquals(-32004, e.code)
    }

    @Test
    fun watchDeliversChanged() {
        val plugins = connect(wants).plugins
        val events = plugins.events.queue()
        val watch = plugins.watch().await()
        assertSent(4, "plugins/watch", "p2pact.plugins", "plugins.watch.request")
        assertEquals("com.example.hello", watch.plugins.single().name)

        engine.notify(4, Fixtures.read("p2pact.plugins", "plugins.changed.notification"))
        val c = events.next() as PluginEvent.Changed
        assertEquals("com.example.hello", c.plugin.name)
        assertEquals(PluginStatus.RUNNING, c.plugin.status)
        assertFalse(c.removed)

        engine.notify(4, Fixtures.read("p2pact.plugins", "plugins.changed.removed.notification"))
        val r = events.next() as PluginEvent.Changed
        assertEquals(PluginStatus.STOPPED, r.plugin.status)
        assertTrue(r.removed)

        watch.close()
        for (i in 0..500) { if (engine.seen.any { it.method == "plugins/unwatch" }) break; Thread.sleep(10) }
        assertSent(4, "plugins/unwatch", "p2pact.plugins", "plugins.unwatch.request")
    }

    @Test
    fun enableDisableWithWrite() {
        val computer = connect(wants + Grant(Tools.PLUGINS, LEVEL_WRITE))
        computer.plugins.enable("com.example.hello").await()
        assertSent(4, "plugins/enable", "p2pact.plugins", "plugins.enable.request")
        computer.plugins.disable("com.example.hello").await()
        assertSent(4, "plugins/disable", "p2pact.plugins", "plugins.disable.request")
    }

    @Test
    fun callsPluginToolByName() {
        val computer = connect(wants)
        val n = computer.toolNumber("com.example.hello")
        assertTrue("a number from 64 up, the computer's own", n != null && n >= 64)
        assertEquals(listOf<Any?>("hello world"), computer.plugins.callTool("com.example.hello", 1, "world").await())

        // a tool the computer does not announce
        val e = try { computer.plugins.callTool("com.example.nothing", 1).await(); null } catch (e: Throwable) { e }
        assertTrue("got $e", e is NoMethodException)
    }

    @Test
    fun toolGrantIsNeeded() {
        val computer = connect() // no grant on the hosted tool
        val e = try { computer.callTool("com.example.hello", 1, "x").await(); null } catch (e: Throwable) { e }
        assertTrue("got $e", e is ForbiddenException)
    }
}
