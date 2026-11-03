package io.github.p2pact.client

import io.github.p2pact.Grant
import io.github.p2pact.LEVEL_READ
import io.github.p2pact.LEVEL_WRITE
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CompletableFuture

private const val CHAT = "p2pact.chat"

class ChatClientTest : ClientTestBase() {
    @Test
    fun pairingGrantsAndState() {
        val computer = connect()
        assertEquals(listOf(computer.id), client.computers().map { it.id })
        assertEquals("MacBook Pro", computer.name)
        assertEquals(ConnectionState.ONLINE, computer.state)
        assertEquals(DEFAULT_WANTS, computer.grants)
        assertEquals(LEVEL_WRITE, computer.level(Tools.CHAT))
        assertEquals(LEVEL_READ, computer.level(Tools.PLUGINS))
        assertFalse(computer.canWrite(Tools.PLUGINS))
        // the engine knows us as a client
        assertEquals(io.github.p2pact.Kind.CLIENT, engine.node.trust.get(engine.clientAddr)!!.kind)
    }

    @Test
    fun initializeAndList() {
        val chat = connect().chat
        val info = chat.initialize().await()
        assertSent(1, "initialize", "p2pact.chat", "initialize.request")
        assertEquals(listOf("claude"), info.agents.map { it.id })
        assertEquals("Claude Code", info.agents[0].name)
        assertTrue(info.loadSession)
        assertTrue(info.extensions)
        assertEquals("MacBook Pro", info.agentTitle)

        val list = chat.listSessions().await()
        assertSent(1, "session/list", "p2pact.chat", "session.list.request")
        val s = list.sessions.single()
        assertEquals("s1", s.id)
        assertEquals("/w/proj", s.cwd)
        assertEquals("claude", s.agent)
        assertEquals(SessionState.WORKING, s.state)
        assertEquals("2026-11-03T10:00:00Z", s.updatedAt)
        assertNull(list.nextCursor)
        // the raw JSON stays reachable
        assertEquals("s1", s.raw["sessionId"])
        assertEquals(listOf(s), chat.sessions)
    }

    @Test
    fun callsInitializeFirstByThemselves() {
        val chat = connect().chat
        chat.listAllSessions().await()
        assertEquals(listOf("initialize", "session/list"), engine.seen.map { it.method })
    }

    @Test
    fun newPromptCancel() {
        val chat = connect().chat
        val created = chat.newSession("/w/app", agent = "codex", model = "gpt-5", text = "собери проект", perms = "ask", mid = "n1").await()
        assertEquals("nomad-1", created.sessionId)
        assertSent(1, "session/new", "p2pact.chat", "session.new.request")

        val result = chat.prompt("s1", "запусти тесты", mid = "m1").await()
        assertEquals("end_turn", result.stopReason)
        assertFalse(result.duplicate)
        assertSent(1, "session/prompt", "p2pact.chat", "session.prompt.request")

        chat.cancel("s1")
        val n = engine.fromClient.next()
        assertEquals("session/cancel", n["method"])
        assertEquals("s1", n.obj("params")!!.str("sessionId"))
    }

    @Test
    fun loadGivesTheWholeHistory() {
        val chat = connect().chat
        val live = chat.updates.queue()
        val loaded = chat.loadSession("s1", "/w/proj").await()
        assertSent(1, "session/load", "p2pact.chat", "session.load.request")
        assertTrue(loaded.older)
        // all three replayed messages are in the result, in order, though the answer may come first
        val u = loaded.updates
        assertEquals(3, u.size)
        val user = u[0] as ChatUpdate.UserMessage
        assertEquals("привет", user.text)
        assertEquals(1L, user.seq)
        val agent = u[1] as ChatUpdate.AgentMessage
        assertEquals("Привет! Чем помочь?", agent.text)
        assertEquals(2L, agent.seq)
        assertEquals("2026-11-03T10:00:01Z", agent.time)
        val tool = u[2] as ChatUpdate.ToolCall
        assertEquals("m3", tool.call.toolCallId)
        assertEquals("Bash: go test", tool.call.title)
        assertEquals("execute", tool.call.kind)
        assertEquals("completed", tool.call.status)
        // and they went to the listeners too
        repeat(3) { live.next() }
    }

    @Test
    fun liveUpdatesAndChanges() {
        val chat = connect().chat
        val updates = chat.updates.queue()
        val changes = chat.sessionChanges.queue()
        engine.notifyChat(Fixtures.read("p2pact.chat", "session.update.agent_message.notification"))
        engine.notifyChat(Fixtures.read("p2pact.chat", "_p2pact.session_changed.notification"))
        val u = updates.next() as ChatUpdate.AgentMessage
        assertEquals("s1", u.sessionId)
        assertEquals("Привет! Чем помочь?", u.text)
        val s = changes.next()
        assertEquals("s1", s.id)
        assertEquals(SessionState.IDLE, s.state)
        assertEquals("claude", s.agent)
        assertEquals("proj", s.title)
        assertEquals("s1", chat.sessions.single().id)
        // closing removes it from the known chats
        engine.notifyChat(mapOf("jsonrpc" to "2.0", "method" to "_p2pact/session_changed",
            "params" to mapOf("sessionId" to "s1", "_meta" to mapOf("p2pact" to mapOf("state" to "closed")))))
        assertEquals(SessionState.CLOSED, changes.next().state)
        assertTrue(chat.sessions.isEmpty())
    }

    @Test
    fun unknownUpdatesAreKeptRaw() {
        val chat = connect().chat
        val updates = chat.updates.queue()
        engine.notifyChat(mapOf("jsonrpc" to "2.0", "method" to "session/update", "params" to mapOf(
            "sessionId" to "s1", "update" to mapOf("sessionUpdate" to "brand_new", "x" to 1, "extra" to listOf("y")))))
        val u = updates.next() as ChatUpdate.Other
        assertEquals("brand_new", u.type)
        assertEquals(1L, u.raw["x"])
    }

    @Test
    fun permissionRequestAnswered() {
        val computer = connect()
        val seen = java.util.concurrent.LinkedBlockingQueue<PermissionRequest>()
        computer.chat.permissionHandler = PermissionHandler { r -> seen.add(r); CompletableFuture.completedFuture(r.allow()) }
        val res = engine.ask(1, Fixtures.read("p2pact.chat", "session.request_permission.request"), "s1").await()
        assertEquals(Fixtures.read("p2pact.chat", "session.request_permission.response"), res)
        val r = seen.next()
        assertEquals("Bash: rm -rf build", r.toolCall.title)
        assertEquals(listOf("allow", "always", "deny"), r.options.map { it.optionId })
        assertEquals(PermissionOutcome.Selected("deny").optionId, (r.deny() as PermissionOutcome.Selected).optionId)
    }

    @Test
    fun questionAnswered() {
        val computer = connect()
        computer.chat.questionHandler = QuestionHandler { q ->
            assertEquals(2, q.questions.size)
            assertEquals("База", q.questions[0].header)
            assertEquals(listOf("Postgres", "SQLite"), q.questions[0].options.map { it.label })
            assertEquals("основная", q.questions[0].options[0].description)
            assertTrue(q.questions[1].multi)
            CompletableFuture.completedFuture(mapOf("База?" to "SQLite", "Что проверить?" to QuestionRequest.join(listOf("Тесты"))))
        }
        val res = engine.ask(1, Fixtures.read("p2pact.chat", "_p2pact.question.request"), "s1").await()
        assertEquals(Fixtures.read("p2pact.chat", "_p2pact.question.response"), res)
    }

    @Test
    fun requestAnsweredElsewhere() {
        val computer = connect()
        val shown = java.util.concurrent.LinkedBlockingQueue<PermissionRequest>()
        // the person never decides here
        computer.chat.permissionHandler = PermissionHandler { r -> shown.add(r); CompletableFuture() }
        val answered = computer.chat.requestAnswered.queue()
        val res = engine.ask(1, Fixtures.read("p2pact.chat", "session.request_permission.request"), "s1")
        val r = shown.next()
        engine.notifyChat(Fixtures.read("p2pact.chat", "_p2pact.request_answered.notification"))
        assertEquals("w1", answered.next().toolCallId)
        r.answered.get(10, java.util.concurrent.TimeUnit.SECONDS)
        // the call does not hang: the computer got an error answer
        assertNotNull(res.await().obj("error"))
    }

    @Test
    fun withoutHandlerTheComputerHearsNoSuchMethod() {
        connect()
        val res = engine.ask(1, Fixtures.read("p2pact.chat", "session.request_permission.request"), "s1").await()
        assertEquals(-32601L, res.obj("error")!!.long("code"))
        val fs = engine.ask(1, mapOf("jsonrpc" to "2.0", "method" to "fs/read_text_file", "params" to mapOf("sessionId" to "s1")), "s1").await()
        assertEquals(-32601L, fs.obj("error")!!.long("code"))
    }

    @Test
    fun extensions() {
        val chat = connect().chat
        chat.close("s1").await()
        assertSent(1, "_p2pact/close", "p2pact.chat", "_p2pact.close.request")
        chat.restart("s1").await()
        chat.hide("s1", true).await()
        assertEquals(mapOf("sessionId" to "s1", "on" to true), engine.seen.last { it.method == "_p2pact/hide" }.params)

        val page = chat.older("s1", 3).await()
        assertSent(1, "_p2pact/older", "p2pact.chat", "_p2pact.older.request")
        val m = page.messages.single()
        assertEquals(1L, m.seq)
        assertEquals("user", m.role)
        assertEquals("привет", m.text)
        assertFalse(page.more)

        val diff = chat.diff("s1").await()
        assertSent(1, "_p2pact/diff", "p2pact.chat", "_p2pact.diff.request")
        assertEquals("a.go", diff.files.single().path)
        assertEquals(3, diff.files.single().added)
        assertEquals("diff --git a/a.go b/a.go\n", diff.patch)
        assertFalse(diff.truncated)

        val image = chat.image("s1", "/w/proj/shot.png").await()
        assertSent(1, "_p2pact/image", "p2pact.chat", "_p2pact.image.request")
        assertEquals("not_found", image.error)
        assertNull(image.data)
    }

    @Test
    fun lateReports() {
        val chat = connect().chat
        val reports = chat.reports.queue()
        engine.notifyChat(Fixtures.read("p2pact.chat", "_p2pact.report.notification"))
        assertEquals("diff_more", reports.next().str("cmd"))
    }

    @Test
    fun readOnlyGrantRefusesWrites() {
        val computer = connect(decide = { listOf(Grant(CHAT, LEVEL_READ), Grant("p2pact.fleet", LEVEL_READ)) })
        assertEquals(LEVEL_READ, computer.level(CHAT))
        assertFalse(computer.canWrite(CHAT))
        val chat = computer.chat
        // reading works
        assertEquals(1, chat.listSessions().await().sessions.size)
        chat.loadSession("s1", "/w/proj").await()
        // writing is the P2PACT error 4, not a JSON-RPC error
        for (write in listOf(
            { chat.newSession("/w/app") },
            { chat.prompt("s1", "x") },
            { chat.close("s1") },
        )) {
            val e = try { write().await(); null } catch (e: Throwable) { e }
            assertTrue("expected Forbidden, got $e", e is ForbiddenException)
            assertEquals(4, (e as ForbiddenException).code)
        }
        // the refused calls did not reach the tool's handler as writes
        assertTrue(engine.seen.none { it.method == "session/prompt" && it.level >= LEVEL_WRITE })
    }

    @Test
    fun errorCodesAreTyped() {
        for ((code, cls) in mapOf(
            1 to NoMethodException::class, 2 to BadArgsException::class, 3 to NotFoundException::class,
            4 to ForbiddenException::class, 5 to FailedException::class, 6 to NotDeliveredException::class,
        )) {
            val t = typed(java.util.concurrent.CompletionException(io.github.p2pact.P2pactError(code, "x")))
            assertEquals(cls, t::class)
            assertEquals(code, (t as P2pactException).code)
        }
        assertEquals(99, (typed(io.github.p2pact.P2pactError(99, "?")) as P2pactException).code)
    }

    @Test
    fun jsonRpcErrorsAreRpcExceptions() {
        val chat = connect().chat
        engine.overrides["1:session/list"] = { mapOf("jsonrpc" to "2.0", "error" to mapOf("code" to -32601, "message" to "nope", "data" to "d")) }
        val e = try { chat.listSessions().await(); null } catch (e: Throwable) { e } as RpcException
        assertTrue(e.isMethodNotFound)
        assertEquals("nope", e.message)
        assertEquals("d", e.data)
    }
}
