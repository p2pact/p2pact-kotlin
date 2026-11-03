package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import java.io.File
import java.util.concurrent.ExecutionException

/** NetTestBase gives the tests of the platform features (kinds, tools, grants, wants, plugins) their nodes. */
abstract class NetTestBase {
    @get:Rule
    val tmp = TemporaryFolder()
    private val nodes = ArrayList<Node>()

    @After
    fun closeAll() = nodes.forEach { it.close() }

    protected fun node(name: String, udp: Boolean = true, dir: File = tmp.newFolder()): Node {
        val n = Node(dir, name)
        if (udp) n.listenUdp("127.0.0.1:0")
        n.handle(64, 1) { _, args -> listOf(args[0]) } // echo
        n.start()
        nodes.add(n)
        return n
    }

    protected fun pair(owner: Node, joiner: Node): Peer {
        val p = joiner.join(Invite.parse(owner.invite(60_000).toString()), 10_000)
        assertEquals(owner.id.addr, p.addr)
        eventually("the owner remembers the joiner") { owner.trust.get(joiner.id.addr) != null }
        return p
    }

    protected fun echo(from: Node, to: Node, data: ByteArray) {
        val res = from.call(to.id.addr, 64, 1, data, timeoutMs = 10_000).get()
        assertArrayEquals(data, res[0] as ByteArray)
    }

    protected fun eventually(what: String, cond: () -> Boolean) {
        for (i in 0..500) {
            if (cond()) return
            Thread.sleep(10)
        }
        fail("timeout: $what")
    }

    /** code is the error code of a failed call; 0 — it succeeded. */
    protected fun code(f: java.util.concurrent.CompletableFuture<*>): Int = try {
        f.get(10, java.util.concurrent.TimeUnit.SECONDS)
        0
    } catch (e: ExecutionException) {
        (e.cause as? P2pactError)?.code ?: -1
    }
}
