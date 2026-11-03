package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

class AfterEventsTest : NetTestBase() {
    // A call's result completes as it arrives, ahead of the events sent before it and still queued
    // behind a slow listener; afterEvents waits for those.
    @Test
    fun waitsForTheEventsBeforeTheResult() {
        val a = node("Mac")
        val b = node("App")
        val got = CopyOnWriteArrayList<Long>()
        b.onEvent = { _, _, _, args -> Thread.sleep(20); got.add(args[0] as Long) }
        a.handle(1, 1) { peer, _ ->
            for (i in 1L..10L) a.eventOn(peer, 7, 1, 1, i)
            listOf("done")
        }
        pair(a, b)
        a.trust.get(b.id.addr) // the pairing is complete on both sides
        val res = b.callOn(a.id.addr, 7, 1, 1, timeoutMs = 10_000).get()
        assertEquals("done", res[0])
        assertTrue("the slow listener is still busy when the result is in", got.size < 10)
        b.afterEvents(a.id.addr, 7).get(10, java.util.concurrent.TimeUnit.SECONDS)
        assertEquals((1L..10L).toList(), got.toList())
    }
}
