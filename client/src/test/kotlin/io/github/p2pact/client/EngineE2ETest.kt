package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Assume.assumeTrue
import org.junit.Test
import java.io.File
import java.nio.file.Files
import java.util.concurrent.CompletableFuture
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * The client SDK against the real Nomad Code engine (Go, `agent/engine`, TestAppE2E): the
 * engine runs as a `go test` process on 127.0.0.1 with a random port, nothing of the user's
 * engine is touched. Only with `P2PACT_ENGINE_E2E=<path to nomad-code/agent>` (Go is needed).
 */
class EngineE2ETest {
    private class Engine(agent: String, decide: String) : AutoCloseable {
        val dir: File = Files.createTempDirectory("app-e2e").toFile()
        val log = File(dir, "engine.log")
        val proc: Process = ProcessBuilder("go", "test", "-count=1", "-v", "-run", "^TestAppE2E$", "./engine")
            .directory(File(agent)).redirectErrorStream(true).redirectOutput(log)
            .also {
                it.environment()["NOMAD_APP_E2E"] = dir.path
                it.environment()["NOMAD_APP_E2E_DECIDE"] = decide
            }.start()

        fun waitFile(name: String, ms: Long = 120_000): String {
            val end = System.currentTimeMillis() + ms
            while (System.currentTimeMillis() < end) {
                val f = File(dir, name)
                if (f.isFile && f.length() > 0) return f.readText()
                Thread.sleep(50)
            }
            fail("no file $name from the engine: ${log.readText().takeLast(3000)}")
            return ""
        }

        fun finish() {
            File(dir, "done").writeText("ok")
            assertTrue("the engine did not finish: ${log.readText().takeLast(2000)}", proc.waitFor(60, TimeUnit.SECONDS))
            assertEquals(log.readText().takeLast(2000), 0, proc.exitValue())
        }

        override fun close() {
            proc.destroy()
            dir.deleteRecursively()
        }
    }

    private fun agentPath(): String {
        val agent = System.getenv("P2PACT_ENGINE_E2E")
        assumeTrue("P2PACT_ENGINE_E2E is not set", !agent.isNullOrEmpty())
        return agent!!
    }

    private fun <T> LinkedBlockingQueue<T>.next(what: String, ms: Long = 20_000): T =
        poll(ms, TimeUnit.MILLISECONDS) ?: throw AssertionError("no $what within $ms ms")

    private val wants = listOf(Grant(Tools.CHAT, LEVEL_WRITE), Grant(Tools.FLEET, LEVEL_READ))

    private fun app(data: File) = P2pactClient(data, "E2E app", ClientConfig(udp = "127.0.0.1:0", tcp = null))

    @Test
    fun appAgainstEngine() {
        val data = Files.createTempDirectory("app-e2e-client").toFile()
        try {
            Engine(agentPath(), "yes").use { engine ->
                app(data).use { client ->
                    val computer = client.pair(engine.waitFile("invite"), wants).await(60_000)
                    computer.whenOnline().await(30_000)
                    assertEquals("approved", engine.waitFile("decision"))

                    val chat = computer.chat
                    assertTrue(chat.initialize().await().loadSession)
                    assertEquals("s1", chat.listSessions().await().sessions.single().id)

                    // typed extension results (v 2)
                    assertEquals(2, chat.info!!.version)
                    assertTrue(chat.agents().await().agents.isNotEmpty())
                    assertTrue(chat.projects().await().dirs.size >= 0)
                    assertEquals("nope.txt", chat.files("s1", "nope.txt").await().query)

                    // history
                    val loaded = chat.loadSession("s1").await()
                    val texts = loaded.updates.mapNotNull {
                        when (it) {
                            is ChatUpdate.UserMessage -> "u:" + it.text
                            is ChatUpdate.AgentMessage -> "a:" + it.text
                            else -> null
                        }
                    }
                    assertTrue(texts.toString(), "u:привет" in texts && "a:Готово" in texts)

                    // fleet: read
                    val fleet = computer.fleet
                    assertEquals("s1", fleet.sessions().await().single().id)
                    val events = LinkedBlockingQueue<FleetEvent>()
                    fleet.events.subscribe { events.add(it) }
                    fleet.watch().await()

                    // the permission handler answers the engine's question
                    val asked = LinkedBlockingQueue<PermissionRequest>()
                    chat.permissionHandler = PermissionHandler { r -> asked.add(r); CompletableFuture.completedFuture(r.allow()) }

                    // the prompt answers the Stop hook
                    engine.waitFile("ready")
                    val prompt = chat.prompt("s1", "из приложения")
                    assertTrue(engine.waitFile("stop").contains("из приложения"))
                    assertEquals("s1", asked.next("permission request").sessionId)
                    assertTrue(engine.waitFile("perm").contains("\"behavior\":\"allow\""))
                    // the turn ends once the permission is answered and the agent is idle again
                    assertEquals("end_turn", prompt.await(30_000).stopReason)

                    // the fleet watch event about the finished turn
                    var seen = false
                    val end = System.currentTimeMillis() + 20_000
                    while (!seen && System.currentTimeMillis() < end) {
                        val e = events.poll(500, TimeUnit.MILLISECONDS) ?: continue
                        if (e is FleetEvent.SessionMessages && e.id == "s1" && e.messages.any { it.text == "Сделано" }) seen = true
                    }
                    assertTrue("no fleet event with the agent's reply", seen)

                    // fleet is read-only for us
                    try {
                        fleet.send("s1", "нельзя").await()
                        fail("session/send must be forbidden")
                    } catch (e: ForbiddenException) {
                    }
                    engine.finish()
                }
            }
        } finally {
            data.deleteRecursively()
        }
    }

    @Test
    fun readOnlyDecisionForbidsPrompt() {
        val data = Files.createTempDirectory("app-e2e-client").toFile()
        try {
            Engine(agentPath(), "read").use { engine ->
                app(data).use { client ->
                    val computer = client.pair(engine.waitFile("invite"), wants).await(60_000)
                    computer.whenOnline().await(30_000)
                    assertEquals("read", engine.waitFile("decision"))
                    val deadline = System.currentTimeMillis() + 10_000
                    while (computer.level(Tools.CHAT) != LEVEL_READ && System.currentTimeMillis() < deadline) Thread.sleep(50)
                    assertEquals(LEVEL_READ, computer.level(Tools.CHAT))
                    assertEquals("s1", computer.chat.listSessions().await().sessions.single().id)
                    try {
                        computer.chat.prompt("s1", "нельзя").await(20_000)
                        fail("prompt must be forbidden")
                    } catch (e: ForbiddenException) {
                    }
                    engine.finish()
                }
            }
        } finally {
            data.deleteRecursively()
        }
    }
}
