package io.github.p2pact

import org.junit.After
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import java.util.concurrent.ExecutionException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.random.Random

class NodeTest {
    @get:Rule
    val tmp = TemporaryFolder()
    private val nodes = ArrayList<Node>()

    @After
    fun closeAll() = nodes.forEach { it.close() }

    private fun node(name: String, tcp: String? = null, udp: Boolean = true, timing: LearnTiming = LearnTiming(), dir: java.io.File = tmp.newFolder()): Node {
        val n = Node(dir, name, timing)
        if (udp) n.listenUdp("127.0.0.1:0")
        if (tcp != null) n.listenTcp(tcp)
        n.handle(64, 1) { _, args -> listOf(args[0]) }
        n.start()
        nodes.add(n)
        return n
    }

    private fun pair(owner: Node, joiner: Node): Peer {
        val p = joiner.join(Invite.parse(owner.invite(60_000).toString()), 10_000)
        assertEquals(owner.id.addr, p.addr)
        for (i in 0 until 100) {
            if (owner.trust.get(joiner.id.addr) != null) return p
            Thread.sleep(20)
        }
        fail("the owner does not know the joiner")
        throw IllegalStateException()
    }

    private fun echo(from: Node, to: Node, data: ByteArray, stream: Long = 0) {
        val res = from.callOn(to.id.addr, stream, 64, 1, data, timeoutMs = 30_000).get()
        assertArrayEquals(data, res[0] as ByteArray)
    }

    @Test
    fun pairCallAndEventsOverUdp() {
        val a = node("a")
        val b = node("b")
        val events = LinkedBlockingQueue<List<Any?>>()
        a.onEvent = { _, tool, ev, args -> events.add(listOf(tool, ev) + args) }
        pair(a, b)
        assertEquals("a", b.trust.get(a.id.addr)!!.name)
        echo(b, a, "hello".toByteArray())
        echo(a, b, ByteArray(200_000) { (it % 251).toByte() }, 2) // a long message on a stream
        for (i in 1..5) b.eventOn(a.id.addr, 1, 64, 2, i)
        for (i in 1..5) assertEquals(listOf<Any?>(64, 2, i.toLong()), events.poll(5, TimeUnit.SECONDS))
        try {
            b.call(a.id.addr, 65, 1, timeoutMs = 5000).get()
            fail("no such method")
        } catch (e: ExecutionException) {
            assertEquals(Errors.NO_METHOD, (e.cause as P2pactError).code)
        }
        assertEquals(listOf("udp"), b.paths(a.id.addr))
    }

    @Test
    fun overTcpDialOnly() {
        val a = node("a", tcp = "127.0.0.1:0", udp = false)
        val b = node("b", tcp = "", udp = false)
        pair(a, b)
        echo(b, a, "over tcp".toByteArray())
        echo(a, b, ByteArray(100_000) { (it * 7).toByte() }, 1)
        assertEquals(listOf("tcp"), a.paths(b.id.addr))
    }

    @Test
    fun knownDevicesReconnectAfterRestart() {
        val dirA = tmp.newFolder()
        val a = node("a", dir = dirA)
        val b = node("b")
        pair(a, b)
        echo(b, a, "before".toByteArray())
        val port = a.udpRoad!!.localAddr
        a.close()
        nodes.remove(a)
        val a2 = Node(dirA, "a")
        a2.listenUdp(port)
        a2.handle(64, 1) { _, args -> listOf(args[0]) }
        a2.start()
        nodes.add(a2)
        echo(b, a2, "after a restart".toByteArray()) // a new session, data in its first packet
        echo(a2, b, "and back".toByteArray())
    }

    @Test
    fun lossyRoad() {
        val a = node("a")
        val b = node("b")
        pair(a, b)
        val rnd = Random(7)
        a.udpRoad!!.drop = { synchronized(rnd) { rnd.nextDouble() < 0.1 } }
        for (i in 0 until 20) echo(b, a, ByteArray(if (i % 4 == 0) 30_000 else 500) { (i + it).toByte() }, (i % 3).toLong())
    }

    @Test
    fun strangersGetSilence() {
        val a = node("a")
        val b = node("b")
        val c = node("c")
        pair(a, b)
        // c knows a's key (as if it had an old invite) but a does not know c
        val before = a.handshakes
        c.trust.put(Peer(a.id.addr, a.id.key.public, "a", randomBytes(32), Hints(udp = listOf(a.udpRoad!!.localAddr))))
        try {
            c.call(a.id.addr, 64, 1, byteArrayOf(1), timeoutMs = 1500).get()
            fail("a stranger got an answer")
        } catch (e: ExecutionException) {
            assertTrue(e.cause is TimeoutException)
        }
        assertEquals(before, a.handshakes)
    }

    @Test
    fun commonDictionaryIsFetchedAndUsed() {
        val a = node("a")
        val b = node("b")
        a.addDict(CommonDict.raw)
        pair(a, b)
        val common = dictId(CommonDict.raw).hex()
        for (i in 0 until 100) {
            if (a.dictState(b.id.addr).using?.hex() == common) break
            echo(b, a, "ping $i".toByteArray())
            Thread.sleep(20)
        }
        assertEquals(common, a.dictState(b.id.addr).using?.hex()) // b fetched it (0.5 dict) and listed it back
        // b lists what it got from a (it belongs to the pair) and shows it to no one else
        assertEquals(listOf(common), b.dictIds(a.id.addr).map { it.hex() })
        assertTrue(b.dictIds("zzzzzzzzzzzzzzzzzzzzzzzzzz").isEmpty())
        echo(a, b, "the agent finished the task and pushed a commit; the tests pass and the build is green.".toByteArray())
    }

    @Test
    fun ownDictionariesBothWays() {
        val fast = LearnTiming(tickMs = 100, lullMs = 200, waitMs = 2000)
        val a = node("a", timing = fast)
        val b = node("b", timing = fast)
        pair(a, b)
        for (m in CompressTest.chat(0, 400)) {
            echo(b, a, m, 1)
        }
        for (n in listOf(a to b, b to a)) {
            val (x, y) = n
            var st = x.dictState(y.id.addr)
            for (i in 0 until 200) {
                if (st.own != null && st.using?.hex() == st.own?.hex() && !st.training) break
                Thread.sleep(50)
                st = x.dictState(y.id.addr)
            }
            assertNotNull(st.own)
            assertEquals(st.own!!.hex(), st.using?.hex())
        }
        for (m in CompressTest.chat(1000, 20)) echo(a, b, m, 2)
    }

    @Test
    fun receiverRestartsWithoutTheDictionary() {
        val a = node("a")
        val dirB = tmp.newFolder()
        val b = node("b", dir = dirB)
        a.addDict(CommonDict.raw)
        b.addDict(CommonDict.raw)
        pair(a, b)
        for (i in 0 until 15) echo(a, b, "warm up $i".toByteArray())
        val common = dictId(CommonDict.raw).hex()
        assertEquals(common, a.dictState(b.id.addr).using?.hex())
        val port = b.udpRoad!!.localAddr
        b.close()
        nodes.remove(b)
        java.io.File(dirB, "net-dicts").deleteRecursively()
        // compressed with a dictionary the restarted device no longer has, sent before it is back
        val text = "the agent finished the task and pushed a commit; the tests pass and the build is green.".toByteArray()
        val pending = a.call(b.id.addr, 64, 1, text, timeoutMs = 30_000)
        Thread.sleep(300)
        val seen = java.util.Collections.synchronizedList(ArrayList<String>())
        val b2 = Node(dirB, "b")
        b2.listenUdp(port)
        b2.handle(64, 1) { _, args -> seen.add(String(args[0] as ByteArray)); listOf(args[0]) }
        b2.start()
        nodes.add(b2)
        assertArrayEquals(text, pending.get()[0] as ByteArray) // held, the dictionary fetched, then run
        for (i in 0 until 30) echo(a, b2, "after $i: the agent finished the task and pushed a commit; the tests pass.".toByteArray())
        assertEquals(31, seen.size) // each call ran exactly once
        assertEquals(seen.toSet().size, seen.size)
    }
}
