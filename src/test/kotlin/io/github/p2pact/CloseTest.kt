package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

class CloseTest : NetTestBase() {
    /**
     * close waits for a call handler in flight: what it writes is written when close returns; a
     * call and an event queued behind it on its stream do not run.
     */
    @Test
    fun closeWaitsForHandlers() {
        val a = node("a")
        val b = node("b")
        pair(a, b)
        val file = File(tmp.newFolder(), "out")
        val started = AtomicBoolean()
        val after = AtomicBoolean()
        a.handle(64, 2) { _, _ ->
            started.set(true)
            Thread.sleep(300)
            file.writeText("done")
            emptyList()
        }
        a.handle(64, 3) { _, _ -> after.set(true); emptyList() }
        a.onEvent = { _, _, _, _ -> after.set(true) }
        b.callOn(a.id.addr, 7, 64, 2, timeoutMs = 5000)
        eventually("the handler started") { started.get() }
        b.eventOn(a.id.addr, 7, 64, 1)
        b.callOn(a.id.addr, 7, 64, 3, timeoutMs = 5000)
        Thread.sleep(100) // both reach a, behind the slow call
        a.close()
        assertEquals("close returned before the handler finished", "done", if (file.exists()) file.readText() else "")
        Thread.sleep(200)
        assertFalse("work queued behind the handler ran after close", after.get())
    }

    /** A handler that does not end holds up close for closeWaitMs at most, then is interrupted. */
    @Test
    fun closeBoundedWait() {
        val a = node("a")
        val b = node("b")
        pair(a, b)
        a.closeWaitMs = 200
        val started = AtomicBoolean()
        val interrupted = AtomicBoolean()
        a.handle(64, 2) { _, _ ->
            started.set(true)
            try { Thread.sleep(60_000) } catch (e: InterruptedException) { interrupted.set(true) }
            emptyList()
        }
        b.callOn(a.id.addr, 1, 64, 2, timeoutMs = 5000)
        eventually("the handler started") { started.get() }
        val t0 = System.nanoTime()
        a.close()
        val ms = (System.nanoTime() - t0) / 1_000_000
        assertTrue("close took $ms ms", ms in 200..2000)
        eventually("the handler was interrupted") { interrupted.get() }
    }
}
