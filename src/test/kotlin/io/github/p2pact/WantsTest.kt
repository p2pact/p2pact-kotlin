package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class WantsTest : NetTestBase() {
    private fun joinWants(inviter: Node, joiner: Node, wants: List<Grant>?) {
        val inv = Invite.parse(inviter.invite(600_000).toString())
        joiner.join(inv, 10_000, wants)
    }

    @Test
    fun askedAreGranted() {
        val a = node("Mac")
        val b = node("App")
        b.kind = Kind.CLIENT
        val want = listOf(Grant("com.acme.x", LEVEL_READ), Grant("p2pact.chat", LEVEL_WRITE))
        joinWants(a, b, want)
        eventually("the inviter remembers the joiner") { a.trust.get(b.id.addr) != null }
        val p = a.trust.get(b.id.addr)!!
        assertEquals(want, p.allowed())
        assertEquals(Kind.CLIENT, p.kind)
        eventually("the newcomer is told") { b.grantedBy(a.id.addr) == want }
    }

    // Pairing without wants asks for everything: behaviour as before, and no event 0.6 is needed.
    @Test
    fun noneIsEverything() {
        val a = node("Mac")
        val b = node("Phone")
        pair(a, b)
        val p = a.trust.get(b.id.addr)!!
        assertNull(p.grants)
        assertEquals(defaultGrants(), p.allowed())
        echo(b, a, "x".toByteArray())
    }

    // The inviting application sees the wants and decides what is granted before it accepts.
    @Test
    fun decidedByApplication() {
        val a = node("Mac")
        val b = node("App")
        val asked = LinkedBlockingQueue<Pair<Peer, List<Grant>>>()
        val paired = LinkedBlockingQueue<Peer>()
        val release = CountDownLatch(1)
        a.onWants = { p, wants ->
            asked.add(p to wants)
            release.await()
            listOf(Grant("com.acme.x", LEVEL_READ))
        }
        a.onPaired = { paired.add(it) }
        b.kind = Kind.CLIENT
        joinWants(a, b, listOf(Grant(ALL_TOOLS, LEVEL_WRITE)))
        val (peer, wants) = asked.poll(3, TimeUnit.SECONDS)!!
        assertEquals("App", peer.name)
        assertEquals(Kind.CLIENT, peer.kind)
        assertEquals(listOf(Grant(ALL_TOOLS, LEVEL_WRITE)), wants)
        // undecided: the newcomer may use nothing
        assertEquals(0, a.grants(b.id.addr)!!.size)
        assertEquals(Errors.FORBIDDEN, code(b.call(a.id.addr, 64, 1, ByteArray(1), timeoutMs = 10_000)))
        release.countDown()
        val p = paired.poll(3, TimeUnit.SECONDS)
        assertTrue("onPaired did not come", p != null)
        assertEquals(listOf(Grant("com.acme.x", LEVEL_READ)), p!!.allowed())
        eventually("narrowed grants told") { b.grantedBy(a.id.addr) == listOf(Grant("com.acme.x", LEVEL_READ)) }
    }

    // No wants in the payload (an older device): the application is shown ["*", 2]; it may refuse.
    @Test
    fun refused() {
        val a = node("Mac")
        val b = node("Old")
        val got = LinkedBlockingQueue<List<Grant>>()
        a.onWants = { _, wants -> got.add(wants); null }
        joinWants(a, b, null)
        assertEquals(defaultGrants(), got.poll(3, TimeUnit.SECONDS))
        eventually("forgotten") { a.trust.get(b.id.addr) == null }
    }

    // A newcomer that asked and is not yet answered may use nothing (kept over a restart); one that did not ask has everything.
    
    fun nothingUntilTold() {
        val dir = tmp.newFolder()
        val a = node("Mac")
        val b = node("App", dir = dir)
        val release = CountDownLatch(1)
        a.onWants = { _, _ -> release.await(); listOf(Grant("com.acme.x", LEVEL_READ)) }
        b.kind = Kind.CLIENT
        joinWants(a, b, listOf(Grant("com.acme.x", LEVEL_WRITE)))
        assertEquals(emptyList<Grant>(), b.grantedBy(a.id.addr))
        assertEquals(emptyList<Grant>(), Trust(dir).get(a.id.addr)!!.granted)
        release.countDown()
        eventually("told") { b.grantedBy(a.id.addr) == listOf(Grant("com.acme.x", LEVEL_READ)) }
        val c = node("Phone")
        a.onWants = null
        joinWants(a, c, null)
        assertEquals(defaultGrants(), c.grantedBy(a.id.addr))
    }

    // Asked for everything and got it: still told by 0.6, or the newcomer would wait for ever.
    
    fun everythingIsTold() {
        val a = node("Mac")
        val b = node("App")
        b.kind = Kind.CLIENT
        joinWants(a, b, listOf(Grant(ALL_TOOLS, LEVEL_WRITE)))
        eventually("told everything") { b.grantedBy(a.id.addr) == defaultGrants() && b.trust.get(a.id.addr)!!.granted != null }
    }
}
