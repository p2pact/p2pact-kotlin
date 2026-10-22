// Interop peer for p2pact-tests: a P2PACT node driven by JSON lines on stdin, answering on
// stdout. Every implementation ships one, so any two can be tested against each other.
//
//   {"cmd":"listen","udp":"127.0.0.1:0","tcp":"127.0.0.1:0"}  → {"addr":"…","udp":"…","tcp":"…"}
//     optional "relay":"host:port","fp":"<hex>" — our relay (pairing and pair channels)
//   {"cmd":"invite"}                                          → {"invite":"p2pact1.…"}
//   {"cmd":"join","invite":"…"}                               → {"peer":"…"}
//   {"cmd":"known","peer":"…"}                                → {"known":true|false}
//   {"cmd":"call","peer":"…","data":"<base64>","stream":N}    → {"data":"<base64>"}  (echo, tool 64 method 1)
//   {"cmd":"dict","raw":"<base64>"}                           → {"ok":true}
//   {"cmd":"dict-state","peer":"…"}                           → {"own":"<base64>"|null,"using":…,"training":bool}
//   {"cmd":"close"}
// Errors: {"error":"…"}. Usage: p2pact-interop [data dir] [name]
package io.github.p2pact.interop

import io.github.p2pact.Errors
import io.github.p2pact.Invite
import io.github.p2pact.Json
import io.github.p2pact.Node
import io.github.p2pact.P2pactError
import io.github.p2pact.RelayHint
import java.io.File
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.ExecutionException
import kotlin.system.exitProcess

fun main(argv: Array<String>) {
    val dir = argv.getOrNull(0)?.let { File(it) } ?: Files.createTempDirectory("p2pact-interop-").toFile()
    val node = Node(dir, argv.getOrNull(1) ?: "kotlin")
    node.handle(64, 1) { _, args ->
        val b = args.getOrNull(0) as? ByteArray ?: if (args.getOrNull(0) == null) ByteArray(0) else
            throw P2pactError(Errors.BAD_ARGS, "no data")
        listOf(b)
    }
    node.start()
    val out = System.out
    fun say(v: Map<String, Any?>) {
        out.print(Json.write(v) + "\n")
        out.flush()
    }
    val enc = Base64.getEncoder()
    val dec = Base64.getDecoder()
    val input = System.`in`.bufferedReader()
    while (true) {
        val line = input.readLine() ?: break
        if (line.isBlank()) continue
        @Suppress("UNCHECKED_CAST")
        val req = try { Json.parse(line) as Map<String, Any?> } catch (e: Exception) { say(mapOf("error" to "bad json")); continue }
        try {
            when (req["cmd"]) {
                "listen" -> {
                    (req["udp"] as? String)?.let { if (it.isNotEmpty()) node.listenUdp(it) }
                    (req["tcp"] as? String)?.let { node.listenTcp(it) }
                    (req["relay"] as? String)?.let { node.setRelay(RelayHint(it, req["fp"] as? String ?: "")) }
                    say(mapOf("addr" to node.id.addr, "udp" to (node.udpRoad?.localAddr ?: ""), "tcp" to (node.tcpRoad?.localAddr ?: "")))
                }
                "invite" -> say(mapOf("invite" to node.invite(600_000).toString()))
                "join" -> say(mapOf("peer" to node.join(Invite.parse(req["invite"] as String), 15_000).addr))
                "known" -> say(mapOf("known" to (node.trust.get(req["peer"] as String) != null)))
                "call" -> {
                    val data = dec.decode(req["data"] as? String ?: "")
                    val stream = (req["stream"] as? Number)?.toLong() ?: 0
                    val res = node.callOn(req["peer"] as String, stream, 64, 1, data, timeoutMs = 60_000).get()
                    val b = res.getOrNull(0) as? ByteArray ?: throw IllegalStateException("invalid result")
                    say(mapOf("data" to enc.encodeToString(b)))
                }
                "dict" -> {
                    node.addDict(dec.decode(req["raw"] as String))
                    say(mapOf("ok" to true))
                }
                "dict-state" -> {
                    val st = node.dictState(req["peer"] as String)
                    say(mapOf("own" to st.own?.let { enc.encodeToString(it) }, "using" to st.using?.let { enc.encodeToString(it) },
                        "training" to st.training))
                }
                "close" -> {
                    node.close()
                    exitProcess(0)
                }
                else -> say(mapOf("error" to "unknown command ${req["cmd"]}"))
            }
        } catch (e: Exception) {
            val c = if (e is ExecutionException) e.cause ?: e else e
            say(mapOf("error" to (c.message ?: c.toString())))
        }
    }
    node.close()
    exitProcess(0)
}
