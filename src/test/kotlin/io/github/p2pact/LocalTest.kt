package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeoutException

class LocalTest : NetTestBase() {
    /** shortDir is a directory with a short path: a unix socket path is limited to about 100 bytes. */
    private fun shortDir(): File = Files.createTempDirectory("p2p").toFile().also { it.deleteOnExit() }

    private fun pluginEnvMap(env: List<String>) = env.associate { it.substringBefore('=') to it.substringAfter('=') }

    @Test
    fun socketPairingAndCalls() {
        assumeTrue(localSocketsSupported())
        val sock = File(shortDir(), "node.sock").path
        val a = node("Mac", udp = false)
        val b = node("Tool", udp = false)
        a.listenLocal(sock)
        val file = File(sock).toPath()
        assertEquals("rw-------", PosixFilePermissions.toString(Files.getPosixFilePermissions(file)))
        try { a.listenLocal(sock); fail("a second listener on the same node") } catch (e: IllegalStateException) { }
        b.useLocal(sock)
        assertEquals(sock, b.localPath())
        pair(a, b)
        echo(b, a, "over the socket".toByteArray())
        echo(a, b, "local ".repeat(2000).toByteArray())
        assertEquals(listOf("local"), b.paths(a.id.addr))
        assertTrue("the dialing side does not remember the road", b.trust.get(a.id.addr)!!.local)
    }

    // A socket file nobody listens on is replaced; one in use is not; something else is not touched.
    @Test
    fun staleSocket() {
        assumeTrue(localSocketsSupported())
        val dir = shortDir()
        val sock = File(dir, "n.sock").path
        val a = Node(tmp.newFolder(), "A")
        a.start()
        a.listenLocal(sock)
        val b = node("B", udp = false)
        try { b.listenLocal(sock); fail("took a socket in use") } catch (e: java.io.IOException) { }
        a.close() // closing the listener removes the file
        assertFalse(File(sock).exists())
        val file = File(dir, "file").also { it.writeText("") }
        try { b.listenLocal(file.path); fail("replaced a regular file") } catch (e: java.io.IOException) { }
        assertTrue(file.exists())
        b.listenLocal(sock)
    }

    // A plugin: the device starts it with the two variables; it pairs with kind 3, gets the
    // install's grants, is reachable over the socket only.
    @Test
    fun pluginPairsByEnv() {
        assumeTrue(localSocketsSupported())
        val sock = File(shortDir(), "host.sock").path
        val host = node("Mac") // it has UDP: a plugin must still never use it
        host.listenLocal(sock)
        host.registerTool("com.acme.host", 1)
        host.handleTool("com.acme.host", 1) { _, _ -> listOf("host") }
        val grants = listOf(Grant("com.acme.host", LEVEL_WRITE))
        val env = host.pluginEnv(3_600_000, grants)
        assertEquals(2, env.size)
        assertTrue(env[0].startsWith("P2PACT_SOCKET=") && env[1].startsWith("P2PACT_INVITE="))
        val inv = Invite.parse(env[1].removePrefix("P2PACT_INVITE="))
        assertTrue("the invite has hints", inv.hints.udp.isEmpty() && inv.hints.tcp.isEmpty() && inv.hints.relay == null)
        var asked = false
        host.onWants = { _, _ -> asked = true; null }
        val plug = newPlugin(tmp.newFolder(), "Plugin", 10_000, pluginEnvMap(env))
        try {
            assertEquals(host.id.addr, plug.host.addr)
            eventually("host trusts the plugin") { host.trust.get(plug.node.id.addr) != null }
            val p = host.trust.get(plug.node.id.addr)!!
            assertEquals(Kind.PLUGIN, p.kind)
            assertEquals(grants, p.allowed())
            assertTrue(p.hints.udp.isEmpty() && p.hints.tcp.isEmpty())
            assertFalse("the person was asked about a plugin", asked)
            // the plugin calls the host within its grants, not outside them
            eventually("hello") { plug.node.toolNumber(host.id.addr, "com.acme.host") != null }
            assertEquals(0, code(plug.node.callTool(host.id.addr, "com.acme.host", 1, timeoutMs = 10_000)))
            assertEquals(Errors.FORBIDDEN, code(plug.node.call(host.id.addr, 64, 1, ByteArray(1), timeoutMs = 10_000)))
            // the host calls the plugin's tool (its grants for the host are the default)
            plug.node.handleTool("com.acme.plug", 1) { _, _ -> listOf("plugin") }
            eventually("plugin hello") { host.toolNumber(plug.node.id.addr, "com.acme.plug") != null }
            assertEquals(listOf<Any?>("plugin"), host.callTool(plug.node.id.addr, "com.acme.plug", 1, timeoutMs = 10_000).get())
            assertEquals(listOf("local"), host.paths(plug.node.id.addr))
            // single use
            try {
                newPlugin(tmp.newFolder(), "Again", 1_500, pluginEnvMap(env)).node.close()
                fail("an invite was used twice")
            } catch (e: TimeoutException) {
            }
            // not a family member
            try { host.befriend(p); fail("a plugin was befriended") } catch (e: IllegalArgumentException) { }
        } finally {
            plug.node.close()
        }
    }

    // A plugin pairing over any other road is refused, and so is a program invite used as something else.
    @Test
    fun pluginOnlyOverLocal() {
        assumeTrue(localSocketsSupported())
        val sock = File(shortDir(), "host.sock").path
        val host = node("Mac")
        val other = node("Other")
        host.listenLocal(sock)
        other.kind = Kind.PLUGIN
        runCatching { other.join(Invite.parse(host.invite(60_000).toString()), 1_500) } // the joiner cannot tell: the host just stays silent
        Thread.sleep(200)
        assertNull(host.trust.get(other.id.addr))
        other.kind = Kind.CLIENT // not a plugin: the invite made for programs is not for it
        other.useLocal(sock)
        runCatching { other.join(Invite.parse(host.pluginInvite(60_000, emptyList()).toString()), 1_500) }
        Thread.sleep(200)
        assertNull(host.trust.get(other.id.addr))
    }

    @Test
    fun pluginEnvNeedsSocket() {
        val n = node("Mac", udp = false)
        try { n.pluginEnv(60_000, emptyList()); fail("env without a socket") } catch (e: IllegalStateException) { }
        try { newPlugin(tmp.newFolder(), "x", 1_000, emptyMap()); fail("a plugin started by nobody") } catch (e: IllegalArgumentException) { }
    }
}
