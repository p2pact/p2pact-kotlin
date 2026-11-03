package io.github.p2pact.client

import io.github.p2pact.Grant
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** A client app paired with a [FakeEngine] in this process. Waits are on events, not on time. */
abstract class ClientTestBase {
    @get:Rule
    val tmp = TemporaryFolder()
    private val closers = ArrayList<AutoCloseable>()
    protected lateinit var engine: FakeEngine
    protected lateinit var client: P2pactClient

    @After
    fun closeAll() = closers.reversed().forEach { it.close() }

    /** Starts the engine and the client; pairs with [wants] and waits until the computer's hello came. */
    protected fun connect(wants: List<Grant> = DEFAULT_WANTS, decide: ((List<Grant>) -> List<Grant>)? = null): Computer {
        engine = FakeEngine(tmp.newFolder()).also { closers.add(it) }
        if (decide != null) engine.decide = decide
        client = P2pactClient(tmp.newFolder(), "Test app", ClientConfig(udp = "127.0.0.1:0", tcp = null)).also { closers.add(it) }
        val grants = client.computerEvents.queue()
        val computer = client.pair(engine.invite(), wants).await(15_000)
        computer.whenOnline().await(15_000)
        // the narrowed or given grants (event 0.6) come right after pairing, unless they are everything
        if (wants.isNotEmpty() && engine.decide(wants) != listOf(Grant("*", 2))) {
            while (true) {
                val e = grants.poll(10, TimeUnit.SECONDS)
                assertNotNull("event 0.6 did not come", e)
                if (e is ComputerEvent.GrantsChanged) break
            }
        }
        return computer
    }

    protected fun <T> Events<T>.queue(): LinkedBlockingQueue<T> {
        val q = LinkedBlockingQueue<T>()
        subscribe { q.add(it) }
        return q
    }

    protected fun <T> LinkedBlockingQueue<T>.next(): T = poll(10, TimeUnit.SECONDS) ?: throw AssertionError("no event within 10 s")

    /** What the client sent for [method] equals the fixture's params. */
    protected fun assertSent(tool: Int, method: String, fixtureDir: String, fixture: String) {
        val s = engine.seen.lastOrNull { it.tool == tool && it.method == method } ?: throw AssertionError("$method was not called")
        assertEquals(method, Fixtures.params(fixtureDir, fixture), s.params)
    }
}
