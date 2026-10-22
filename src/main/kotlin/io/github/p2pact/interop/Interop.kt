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
//   {"cmd":"road","listen":"127.0.0.1:0","mtu":20,"rate":N}   → {"road":"127.0.0.1:…"}
//   {"cmd":"road","dial":"127.0.0.1:…","mtu":20,"rate":N,"peer":"…"} → {"ok":true}
//     an application road (stream) over a local TCP connection that stands in for a Bluetooth LE
//     link: writes of at most mtu bytes, paced to rate bytes a second (0 — not paced); the
//     listening side attaches the first connection it accepts; peer optional (bound road)
//   {"cmd":"close"}
// Errors: {"error":"…"}. Usage: p2pact-interop [data dir] [name]
package io.github.p2pact.interop

import io.github.p2pact.AppRoad
import io.github.p2pact.AppRoadConfig
import io.github.p2pact.Errors
import io.github.p2pact.Invite
import io.github.p2pact.Json
import io.github.p2pact.Node
import io.github.p2pact.P2pactError
import io.github.p2pact.RelayHint
import java.io.File
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
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
                "road" -> {
                    val cfg = AppRoadConfig("ble", stream = true, mtu = (req["mtu"] as Number).toInt(), rate = (req["rate"] as? Number)?.toInt() ?: 0)
                    val listen = req["listen"] as? String
                    if (listen != null) {
                        val i = listen.lastIndexOf(':')
                        val ss = ServerSocket()
                        ss.bind(InetSocketAddress(InetAddress.getByName(listen.substring(0, i)), listen.substring(i + 1).toInt()))
                        Thread {
                            try {
                                val c = ss.accept()
                                ss.close()
                                linkRoad(node, c, null, cfg)
                            } catch (e: Exception) {
                            }
                        }.apply { isDaemon = true }.start()
                        say(mapOf("road" to "${ss.inetAddress.hostAddress}:${ss.localPort}"))
                    } else {
                        val dial = req["dial"] as String
                        val i = dial.lastIndexOf(':')
                        linkRoad(node, Socket(dial.substring(0, i), dial.substring(i + 1).toInt()), req["peer"] as? String, cfg)
                        say(mapOf("ok" to true))
                    }
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

/** PipeLink is a Bluetooth LE link stood in for by a TCP connection: send writes at most the MTU at a time, at the link's rate. */
private class PipeLink(val socket: Socket, val rate: Int) : AppRoad {
    private val os = socket.getOutputStream()
    private var free = 0L

    override fun send(b: ByteArray) {
        if (rate > 0) {
            val now = System.nanoTime()
            if (free < now - 10_000_000) free = now // idle: the link starts now (an oversleep does not slow it)
            free += b.size * 1_000_000_000L / rate
            val wait = free - System.nanoTime()
            if (wait > 2_000_000) Thread.sleep(wait / 1_000_000, (wait % 1_000_000).toInt())
        }
        os.write(b)
        os.flush()
    }
}

/** linkRoad attaches a connection as an application road and feeds it what arrives, in pieces of at most the MTU, as GATT delivers them. */
private fun linkRoad(node: Node, socket: Socket, peer: String?, cfg: AppRoadConfig) {
    socket.tcpNoDelay = true
    val road = try {
        node.attachRoad(peer, PipeLink(socket, cfg.rate), cfg)
    } catch (e: Exception) {
        socket.close()
        throw e
    }
    Thread({
        val inp = socket.getInputStream()
        val buf = ByteArray(cfg.mtu)
        try {
            while (true) {
                val k = inp.read(buf)
                if (k < 0) break
                road.receive(buf.copyOf(k))
            }
        } catch (e: Exception) {
        } finally {
            road.detach()
            socket.close()
        }
    }, "p2pact-interop-link").apply { isDaemon = true }.start()
}
