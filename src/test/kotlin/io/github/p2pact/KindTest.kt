package io.github.p2pact

import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class KindTest : NetTestBase() {
    @Test
    fun kindInPairingAndHello() {
        val a = node("Mac")
        val b = node("Phone")
        a.kind = Kind.COMPUTER
        b.kind = Kind.CLIENT
        pair(a, b)
        eventually("kinds in trust") {
            a.trust.get(b.id.addr)?.kind == Kind.CLIENT && b.trust.get(a.id.addr)?.kind == Kind.COMPUTER
        }
        // a later change of kind arrives with the next hello
        b.kind = Kind.PLUGIN
        b.sendInfoForTest(a.id.addr)
        eventually("kind from a new hello") { a.trust.get(b.id.addr)?.kind == Kind.PLUGIN }
    }

    @Test
    fun oldPeerWithoutKind() {
        val a = node("Mac")
        val b = node("Old")
        val old = Cbor.encode(listOf("Old", 7, Hints(udp = listOf("1.2.3.4:5")).toWire(), ByteArray(1), "linux"))
        a.parsePair(old).let {
            assertEquals("Old", it.name); assertEquals(7L, it.boot); assertEquals("linux", it.os)
            assertEquals(Kind.UNKNOWN, it.kind); assertEquals(1, it.hints.udp.size)
        }
        // a newer payload with fields we do not know yet is read too
        val newer = Cbor.encode(listOf("New", 7, Hints().toWire(), ByteArray(1), "linux", Kind.CLIENT, listOf(listOf("*", 1)), "future"))
        a.parsePair(newer).let { assertEquals(Kind.CLIENT, it.kind); assertEquals("New", it.name) }
        // a node that does not say its kind sends the old payload
        assertEquals(5, Cbor.decode(a.pairPayload()).asList()!!.size)
        b.kind = Kind.CLIENT
        pair(a, b)
        eventually("kind") { a.trust.get(b.id.addr)?.kind == Kind.CLIENT }
        // an old hello (no kind) changes nothing but the usual
        val hello = Cbor.encode(listOf(3, 0, 2, "Old", "linux", "old-impl", emptyMap<Int, Int>(), emptyList<ByteArray>(), Hints().toWire()))
        a.deliverForTest(b.id.addr, hello)
        val p = a.trust.get(b.id.addr)!!
        assertEquals("Old", p.name)
        assertEquals(Kind.CLIENT, p.kind)
        echo(a, b, "still works".toByteArray())
    }

    @Test
    fun kindSurvivesRestart() {
        val dir = tmp.newFolder()
        val a = node("Mac", dir = dir)
        val b = node("Phone")
        b.kind = Kind.CLIENT
        pair(a, b)
        eventually("kind") { a.trust.get(b.id.addr)?.kind == Kind.CLIENT }
        a.trust.flush()
        assertTrue(Trust(dir).get(b.id.addr)!!.kind == Kind.CLIENT)
    }
}
