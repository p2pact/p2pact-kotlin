package io.github.p2pact.cli

import io.github.p2pact.client.ChatUpdate
import io.github.p2pact.client.ClientConfig
import io.github.p2pact.client.Computer
import io.github.p2pact.client.ConnectionState
import io.github.p2pact.client.FleetEvent
import io.github.p2pact.client.P2pactClient
import io.github.p2pact.client.PermissionHandler
import io.github.p2pact.client.PermissionOutcome
import io.github.p2pact.client.await
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

private const val USAGE = """p2pact-cli: a small client of the P2PACT engine

  p2pact-cli pair <invite>          pair with a computer (invite: p2pact1.… from `nomad invite`)
  p2pact-cli chats                  list the chats of the paired computers
  p2pact-cli send <session> <text>  send a message and print the reply
  p2pact-cli watch                  print the changes of the fleet and the chats as they come

State (keys, paired computers) is kept in ${'$'}P2PACT_CLI_HOME, by default ~/.p2pact-cli."""

fun main(args: Array<String>) {
    if (args.isEmpty() || args[0] in setOf("-h", "--help", "help")) {
        println(USAGE)
        exitProcess(if (args.isEmpty()) 2 else 0)
    }
    val home = File(System.getenv("P2PACT_CLI_HOME") ?: (System.getProperty("user.home") + "/.p2pact-cli"))
    val client = P2pactClient(home, "p2pact-cli", ClientConfig())
    val code = try {
        when (args[0]) {
            "pair" -> pair(client, args.getOrNull(1) ?: usage())
            "chats" -> chats(client)
            "send" -> send(client, args.getOrNull(1) ?: usage(), args.drop(2).joinToString(" ").ifEmpty { usage() })
            "watch" -> watch(client)
            else -> usage()
        }
    } catch (e: Exception) {
        System.err.println("error: ${e.message ?: e}")
        1
    } finally {
        client.close()
    }
    exitProcess(code)
}

private fun usage(): Nothing {
    System.err.println(USAGE)
    exitProcess(2)
}

private fun pair(client: P2pactClient, invite: String): Int {
    val computer = client.pair(invite).await()
    println("paired with ${computer.name} (${computer.id})")
    // the person at the computer decides what we may use; it tells us right after
    runCatching { computer.whenOnline().await(10_000) }
    Thread.sleep(1000)
    println("grants: " + computer.grants.joinToString { "${it.tool}:${if (it.level >= 2) "write" else "read"}" })
    return 0
}

/** The computers that answer within a few seconds. */
private fun online(client: P2pactClient): List<Computer> {
    val all = client.computers()
    if (all.isEmpty()) System.err.println("no paired computers: run `p2pact-cli pair <invite>` first")
    return all.filter { c ->
        runCatching { c.whenOnline().await(15_000) }
        (c.state == ConnectionState.ONLINE).also { if (!it) System.err.println("${c.name}: offline") }
    }
}

private fun chats(client: P2pactClient): Int {
    for (c in online(client)) {
        println("# ${c.name}")
        for (s in c.chat.listAllSessions().await()) {
            println("${s.id}\t${s.state.wire}\t${s.agent ?: "-"}\t${s.title ?: ""}\t${s.cwd ?: ""}")
        }
    }
    return 0
}

private fun send(client: P2pactClient, sessionId: String, text: String): Int {
    for (c in online(client)) {
        val chat = c.chat
        if (chat.listAllSessions().await().none { it.id == sessionId }) continue
        // the agent may ask: answer on the terminal
        chat.permissionHandler = PermissionHandler { r ->
            System.err.print("\n${r.toolCall.title ?: "permission"}: allow? [y/N] ")
            CompletableFuture.completedFuture(if (readlnOrNull()?.trim()?.lowercase() == "y") r.allow() else r.deny())
        }
        chat.updates.subscribe { u ->
            if (u.sessionId == sessionId && u is ChatUpdate.AgentMessage) {
                print(u.text)
                System.out.flush()
            }
        }
        val result = chat.prompt(sessionId, text).await(0)
        println("\n[${result.stopReason}]")
        return 0
    }
    System.err.println("no such chat: $sessionId")
    return 1
}

private fun watch(client: P2pactClient): Int {
    val computers = online(client)
    for (c in computers) {
        c.fleet.events.subscribe { e ->
            when (e) {
                is FleetEvent.SessionsChanged -> e.sessions.forEach { println("${c.name}: ${it.id} ${it.phase} ${it.title ?: ""}") }
                is FleetEvent.SessionMessages -> e.messages.forEach { println("${c.name}: ${e.id} [${it.role}] ${it.text}") }
                is FleetEvent.Notify -> println("${c.name}: notify: ${e.text}")
                is FleetEvent.Resync -> println("${c.name}: ${e.id} resync")
                else -> {}
            }
        }
        val w = c.fleet.watch().await()
        println("# ${c.name}: ${w.sessions.size} sessions, watching")
    }
    if (computers.isEmpty()) return 1
    CountDownLatch(1).await() // until Ctrl-C
    return 0
}
