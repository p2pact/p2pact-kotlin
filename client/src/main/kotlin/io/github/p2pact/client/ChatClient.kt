package io.github.p2pact.client

import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

/**
 * ChatClient is tool 1 `p2pact.chat` of one computer: ACP over P2PACT with the `_p2pact/<name>`
 * extensions (chat spec). Methods that read need level 1 on the tool, the others level 2; a
 * method the grants refuse fails with [ForbiddenException].
 *
 * `initialize` goes first by itself (and again after the computer's hello comes anew). All
 * calls about one session ride the session's own stream, as the spec says.
 */
class ChatClient internal constructor(private val computer: Computer) {
    private val node get() = computer.owner.node

    /** Live messages of chats (and the history while a session is loaded). */
    val updates = Events<ChatUpdate>()

    /** A chat appeared, changed state or title, or closed (`_p2pact/session_changed`). */
    val sessionChanges = Events<ChatSession>()

    /** Another client answered a request: stop showing it. */
    val requestAnswered = Events<RequestAnswered>()

    /** Late lines of extension commands (`_p2pact/report`): JSON objects with `cmd`. */
    val reports = Events<JsonMap>()

    /** Answers `session/request_permission`; without one the computer gets "no such method" and asks elsewhere. */
    @Volatile var permissionHandler: PermissionHandler? = null

    /** Answers `_p2pact/question`. */
    @Volatile var questionHandler: QuestionHandler? = null

    private val known = ConcurrentHashMap<String, ChatSession>()
    private val collectors = CopyOnWriteArrayList<Pair<String, MutableList<ChatUpdate>>>()
    private val pending = ConcurrentHashMap<String, CompletableFuture<Unit>>()
    @Volatile private var initialized: CompletableFuture<InitializeResult>? = null

    /** The chats seen so far (from lists and changes), without the closed. */
    val sessions: List<ChatSession> get() = known.values.toList()

    /** What `initialize` said, once it has. */
    val info: InitializeResult? get() = initialized?.takeIf { it.isDone && !it.isCompletedExceptionally }?.getNow(null)

    // --- connection ---

    /**
     * initialize says hello to the computer's chat: capabilities, the agents it offers. It also asks
     * for the extension notifications (`_meta.p2pact.ext: 1`). The other calls do it for you.
     */
    fun initialize(): CompletableFuture<InitializeResult> {
        val params = mapOf(
            "protocolVersion" to 1,
            "clientCapabilities" to emptyMap<String, Any?>(),
            "_meta" to mapOf("p2pact" to mapOf("ext" to 1, "v" to 2)),
        )
        val f = computer.rpc(Tools.CHAT_NUMBER, "initialize", params).map { InitializeResult.parse(it) }
        initialized = f
        return f
    }

    internal fun reconnected() {
        initialized = null
    }

    private fun <T> call(method: String, params: JsonMap?, stream: Long = 0, timeoutMs: Long = computer.owner.config.callTimeoutMs, parse: (JsonMap) -> T): CompletableFuture<T> {
        val i = initialized?.takeIf { !it.isCompletedExceptionally } ?: initialize()
        return i.flatMap { computer.rpc(Tools.CHAT_NUMBER, method, params, stream, timeoutMs) }.map(parse)
    }

    // --- chats ---

    /** listSessions is `session/list`: one page. */
    fun listSessions(cwd: String? = null, cursor: String? = null): CompletableFuture<SessionList> {
        val p = LinkedHashMap<String, Any?>()
        if (cwd != null) p["cwd"] = cwd
        if (cursor != null) p["cursor"] = cursor
        return call("session/list", p) { r ->
            val list = r.objs("sessions").map { ChatSession.parse(it) }
            list.forEach { remember(it) }
            SessionList(list, r.str("nextCursor"), r)
        }
    }

    /** listAllSessions follows the cursor to the end. */
    fun listAllSessions(cwd: String? = null): CompletableFuture<List<ChatSession>> {
        val all = ArrayList<ChatSession>()
        fun page(cursor: String?): CompletableFuture<List<ChatSession>> = listSessions(cwd, cursor).flatMap { l ->
            all.addAll(l.sessions)
            if (l.nextCursor.isNullOrEmpty() || l.sessions.isEmpty()) CompletableFuture.completedFuture<List<ChatSession>>(all) else page(l.nextCursor)
        }
        return page(null)
    }

    /**
     * newSession is `session/new`. [agent] is one of [InitializeResult.agents] (default: the first),
     * [model] one of the agent's models, [text] the first task (sent at once), [perms] `"skip"`
     * or `"ask"`. [mid] makes a repeat after a restart harmless.
     */
    fun newSession(
        cwd: String,
        agent: String? = null,
        model: String? = null,
        text: String? = null,
        perms: String? = null,
        mid: String = newMid(),
    ): CompletableFuture<NewSession> {
        val meta = LinkedHashMap<String, Any?>()
        if (agent != null) meta["agent"] = agent
        meta["mid"] = mid
        if (model != null) meta["model"] = model
        if (text != null) meta["text"] = text
        if (perms != null) meta["perms"] = perms
        val params = mapOf("cwd" to cwd, "mcpServers" to emptyList<Any?>(), "_meta" to mapOf("p2pact" to meta))
        return call("session/new", params) { NewSession(it.str("sessionId") ?: throw FailedException("no sessionId in the answer"), it) }
    }

    /**
     * loadSession is `session/load`: the chat's history comes as updates (also to [updates]) and
     * is in the result when it completes. A long chat comes as its tail: [LoadedSession.older];
     * [older] fetches the earlier part.
     */
    fun loadSession(sessionId: String, cwd: String = ""): CompletableFuture<LoadedSession> {
        val stream = streamOf(sessionId)
        val got = ArrayList<ChatUpdate>()
        val entry = sessionId to (java.util.Collections.synchronizedList(got) as MutableList<ChatUpdate>)
        collectors.add(entry)
        val params = mapOf("sessionId" to sessionId, "cwd" to cwd, "mcpServers" to emptyList<Any?>())
        val result = call("session/load", params, stream) { it }
        // the result may come ahead of the last updates queued before it: wait for them
        val done = result.flatMap { r -> node.afterEvents(computer.id, stream).map { r } }
        done.whenComplete { _, _ -> collectors.remove(entry) }
        return done.map { r -> LoadedSession(sessionId, synchronized(got) { got.toList() }, r.p2pactMeta().bool("older") ?: false, r) }
    }

    /**
     * prompt is `session/prompt`: it completes when the agent's turn ends (minutes, no timeout);
     * the reply comes as [updates] meanwhile. [mid] is kept for repeats: send the same one if
     * you do not know whether the first went.
     */
    fun prompt(sessionId: String, text: String, mid: String = newMid()): CompletableFuture<PromptResult> =
        prompt(sessionId, listOf(ContentBlock.text(text)), mid)

    fun prompt(sessionId: String, blocks: List<JsonMap>, mid: String = newMid()): CompletableFuture<PromptResult> {
        val params = mapOf("sessionId" to sessionId, "prompt" to blocks, "_meta" to mapOf("p2pact" to mapOf("mid" to mid)))
        return call("session/prompt", params, streamOf(sessionId), 0) { PromptResult(it.str("stopReason"), it.bool("duplicate") ?: false, it) }
    }

    /** cancel is the notification `session/cancel`: the agent stops the turn. */
    fun cancel(sessionId: String) {
        computer.notify(Tools.CHAT_NUMBER, "session/cancel", mapOf("sessionId" to sessionId), streamOf(sessionId))
    }

    fun setMode(sessionId: String, modeId: String): CompletableFuture<JsonMap> =
        call("session/set_mode", mapOf("sessionId" to sessionId, "modeId" to modeId), streamOf(sessionId)) { it }

    fun setModel(sessionId: String, modelId: String): CompletableFuture<JsonMap> =
        call("session/set_model", mapOf("sessionId" to sessionId, "modelId" to modelId), streamOf(sessionId)) { it }

    /**
     * request is any ACP or extension method that has no typed call: the `result` object. It rides
     * the stream of the params' `sessionId`, or [stream] when given (0: the computer's own stream).
     * Like [loadSession], it completes after the events the computer sent before the result have
     * been handled.
     */
    fun request(method: String, params: JsonMap, stream: Long? = null): CompletableFuture<JsonMap> =
        settledCall(method, params, stream ?: streamOf(params.str("sessionId"))) { it }

    /** call, then wait for the events queued on the stream before the result (a result may come ahead of them). */
    private fun <T> settledCall(method: String, params: JsonMap?, stream: Long, parse: (JsonMap) -> T): CompletableFuture<T> =
        call(method, params, stream) { it }.flatMap { r -> node.afterEvents(computer.id, stream).map { r } }.map(parse)

    // --- extensions (chat spec §10) ---

    /**
     * command calls `_p2pact/<name>` and gives its lines (§9.3): JSON objects with `cmd`. Lines that
     * come later arrive in [reports]. A failure the person should see is the line `{"cmd":"alert"}`.
     */
    fun command(name: String, params: JsonMap = emptyMap()): CompletableFuture<List<JsonMap>> =
        settledCall("_p2pact/$name", params, streamOf(params.str("sessionId"))) { it.objs("lines") }

    /** close closes the chat: it leaves the list. */
    fun close(sessionId: String) = command("close", mapOf("sessionId" to sessionId))

    /** restart starts the agent of a chat again. */
    fun restart(sessionId: String) = command("restart", mapOf("sessionId" to sessionId))

    /** hide hides a chat from the list (on) or shows it again. */
    fun hide(sessionId: String, on: Boolean = true) = command("hide", mapOf("sessionId" to sessionId, "on" to on))

    // Typed results (chat spec §10): the engine answers with typed fields next to `lines`; an older engine
    // with `lines` only is read from the line with the method's `cmd`. A line the engine sends later than
    // its result (an older engine, or a client without `v: 2`) is not waited for: the typed value is then
    // empty, and the content comes to [reports].

    /** The whole `result` of `_p2pact/<name>`, after the events queued before it. */
    private fun ext(name: String, params: JsonMap): CompletableFuture<JsonMap> =
        settledCall("_p2pact/$name", params, streamOf(params.str("sessionId"))) { it }

    /** The typed result if it has one of [keys]; else the line [cmd] of `lines` (older engine); else the result. */
    private fun body(r: JsonMap, cmd: String, vararg keys: String): JsonMap =
        if (keys.any { it in r }) r else r.objs("lines").firstOrNull { it.str("cmd") == cmd } ?: r

    private fun action(name: String, params: JsonMap): CompletableFuture<ActionResult> = ext(name, params).map { r ->
        val lines = r.objs("lines")
        val alert = lines.firstOrNull { it.str("cmd") == "alert" }
        val ok = r.bool("ok") ?: (alert == null)
        ActionResult(ok, r.str("error") ?: if (ok) null else alert?.str("text") ?: alert?.str("msg"), lines, r)
    }

    /** closeSession is [close] with a typed result. */
    fun closeSession(sessionId: String): CompletableFuture<ActionResult> = action("close", mapOf("sessionId" to sessionId))

    /** restartSession is [restart] with a typed result. */
    fun restartSession(sessionId: String): CompletableFuture<ActionResult> = action("restart", mapOf("sessionId" to sessionId))

    /** hideSession is [hide] with a typed result. */
    fun hideSession(sessionId: String, on: Boolean = true): CompletableFuture<ActionResult> = action("hide", mapOf("sessionId" to sessionId, "on" to on))

    /** historyOpen is `_p2pact/history_open`: opens a past conversation of an agent as a chat ([historyId] is a [HistoryItem.id]). */
    fun historyOpen(historyId: String): CompletableFuture<ActionResult> = action("history_open", mapOf("sessionId" to historyId))

    /** older is `_p2pact/older`: the page of messages just before `before` (the `seq` of the oldest you have). */
    fun older(sessionId: String, before: Long): CompletableFuture<OlderPage> =
        ext("older", mapOf("sessionId" to sessionId, "before" to before)).map { r ->
            val l = body(r, "older", "messages")
            OlderPage(sessionId, l.objs("messages").map { m ->
                ChatMessage(m.long("seq") ?: 0, m.str("role") ?: "", m.str("text") ?: "", m.str("time"), m["images"].asList()?.mapNotNull { it as? String } ?: emptyList(), m)
            }, l.bool("more") ?: false, l, l.str("run"))
        }

    /** commands is `_p2pact/commands`: the slash commands of the chat's agent. */
    fun commands(sessionId: String): CompletableFuture<List<SlashCommand>> =
        ext("commands", mapOf("sessionId" to sessionId)).map { r ->
            val l = body(r, "commands", "commands")
            (if ("commands" in l) l.objs("commands") else l.objs("list")).map { c ->
                SlashCommand(c.str("name") ?: "", c.str("desc"), c.str("hint"), c.objs("args").map { SlashArg(it.str("value") ?: "", it.str("desc"), it) }, c)
            }
        }

    /** files is `_p2pact/files`: paths in the chat's folder matching [query]. */
    fun files(sessionId: String, query: String): CompletableFuture<FilesResult> =
        ext("files", mapOf("sessionId" to sessionId, "query" to query)).map { r ->
            val l = body(r, "files", "files")
            val list = (l["files"] ?: l["list"]).asList()?.mapNotNull { it as? String } ?: emptyList()
            FilesResult(list, l.str("query") ?: query, l)
        }

    /** history is `_p2pact/history`: past conversations of the agents, [offset] pages by the page's `cursor`. */
    fun history(query: String = "", offset: Int = 0): CompletableFuture<HistoryPage> =
        ext("history", mapOf("query" to query, "offset" to offset)).map { r ->
            val l = body(r, "history", "items")
            val items = (if ("items" in l) l.objs("items") else l.objs("list")).map {
                HistoryItem(it.str("id") ?: "", it.str("agent") ?: "", it.str("dir") ?: "", it.str("title") ?: "", it.long("updated") ?: 0, it.int("count") ?: 0, it)
            }
            HistoryPage(items, l.str("query") ?: query, l.int("from") ?: offset, l.int("total") ?: items.size, l.int("cursor"), l.bool("partial") ?: false, l)
        }

    /** projects is `_p2pact/projects`: the folders and projects of the computer; [cwd] narrows to one folder's project. */
    fun projects(cwd: String? = null): CompletableFuture<ProjectsResult> =
        ext("projects", if (cwd != null) mapOf("cwd" to cwd) else emptyMap()).map { r ->
            val l = body(r, "projects", "dirs", "project_dirs", "dir_repos")
            ProjectsResult(
                l["dirs"].asList()?.mapNotNull { it as? String } ?: emptyList(),
                l.obj("project_dirs")?.mapValues { (_, v) -> v.asList()?.mapNotNull { it as? String } ?: emptyList() } ?: emptyMap(),
                l.obj("dir_repos")?.mapNotNull { (k, v) -> (v as? String)?.let { k to it } }?.toMap() ?: emptyMap(),
                l,
            )
        }

    /** agents is `_p2pact/agents`: the agents the computer can start and the presets. */
    fun agents(): CompletableFuture<AgentsResult> =
        ext("agents", emptyMap()).map { r ->
            val lines = r.objs("lines")
            fun listOf(cmd: String) = lines.firstOrNull { it.str("cmd") == cmd }?.objs("list") ?: emptyList()
            val agents = if ("agents" in r) r.objs("agents") else listOf("agents")
            val presets = if ("presets" in r) r.objs("presets") else listOf("presets")
            AgentsResult(agents.map { a ->
                AgentEntry(a.str("id") ?: "", a.str("name") ?: a.str("id") ?: "", a.str("type"), a.bool("enabled") ?: true,
                    a["models"].asList()?.mapNotNull { it as? String } ?: emptyList(), a.str("default"), a)
            }, presets.map { AgentPreset(it.str("id") ?: "", it.str("name") ?: "", it) }, r)
        }

    /** diff is `_p2pact/diff`: the changes in the chat's folder (git). */
    fun diff(sessionId: String): CompletableFuture<DiffResult> =
        ext("diff", mapOf("sessionId" to sessionId)).map { r ->
            val l = body(r, "diff", "files", "error")
            DiffResult(l.objs("files").map { DiffFile(it.str("path") ?: "", it.int("added") ?: 0, it.int("removed") ?: 0, it.str("status")) },
                l.str("patch") ?: "", l.bool("truncated") ?: false, l.str("error"), l)
        }

    /** image is `_p2pact/image`: the bytes (base64) of an image a message points to. */
    fun image(sessionId: String, path: String): CompletableFuture<ImageResult> =
        ext("image", mapOf("sessionId" to sessionId, "path" to path)).map { r ->
            val l = body(r, "image", "data", "error")
            ImageResult(l.str("data"), l.str("error"), l, l.str("path") ?: path, l.str("mime"))
        }

    // --- from the computer ---

    private fun remember(s: ChatSession) {
        if (s.id.isEmpty()) return
        if (s.state == SessionState.CLOSED) known.remove(s.id) else known[s.id] = s
    }

    /** A `notify` event (`session/update`, `_p2pact/…`). */
    internal fun notified(args: List<Any?>) {
        val msg = normalize(args.firstOrNull()).asMap() ?: return
        val params = msg.obj("params") ?: emptyMap()
        when (msg.str("method")) {
            "session/update" -> {
                val sid = params.str("sessionId") ?: return
                val u = ChatUpdate.parse(sid, params.obj("update") ?: return)
                for ((id, list) in collectors) if (id == sid) list.add(u)
                updates.emit(u)
            }
            "_p2pact/session_changed" -> {
                val s = ChatSession.parse(params)
                remember(s)
                sessionChanges.emit(s)
            }
            "_p2pact/request_answered" -> {
                val sid = params.str("sessionId") ?: return
                val tc = params.str("toolCallId") ?: return
                pending.remove("$sid/$tc")?.complete(Unit)
                requestAnswered.emit(RequestAnswered(sid, tc))
            }
            // _nomad/…: an engine older than the ext flag
            "_p2pact/report", "_nomad/report" -> params.obj("line")?.let { reports.emit(it) }
        }
    }

    /** A request of the computer (`1.1 rpc`): the answer as a JSON-RPC message. */
    internal fun incoming(args: List<Any?>): CompletableFuture<List<Any?>> {
        val req = normalize(args.firstOrNull()).asMap() ?: return CompletableFuture.completedFuture(error(RpcException.INVALID_PARAMS, "bad request"))
        val params = req.obj("params") ?: emptyMap()
        return when (req.str("method")) {
            "session/request_permission" -> {
                val h = permissionHandler ?: return CompletableFuture.completedFuture(error(RpcException.METHOD_NOT_FOUND, "method not found"))
                val sid = params.str("sessionId") ?: ""
                val call = ToolCallInfo.parse(params.obj("toolCall") ?: emptyMap())
                val answered = CompletableFuture<Unit>()
                pending["$sid/${call.toolCallId}"] = answered
                val request = PermissionRequest(sid, call, params.objs("options").map {
                    PermissionOption(it.str("optionId") ?: "", it.str("name") ?: "", it.str("kind"), it.p2pactMeta().str("description"), it)
                }, params, answered)
                ask(answered, "$sid/${call.toolCallId}", { h.onPermission(request) }) { o ->
                    result(mapOf("outcome" to when (o) {
                        is PermissionOutcome.Selected -> mapOf("outcome" to "selected", "optionId" to o.optionId)
                        PermissionOutcome.Cancelled -> mapOf("outcome" to "cancelled")
                    }))
                } 
            }
            "_p2pact/question", "_nomad/question" -> { // _nomad/…: an engine older than the ext flag
                val h = questionHandler ?: return CompletableFuture.completedFuture(error(RpcException.METHOD_NOT_FOUND, "method not found"))
                val sid = params.str("sessionId") ?: ""
                val tc = params.str("toolCallId") ?: ""
                val answered = CompletableFuture<Unit>()
                pending["$sid/$tc"] = answered
                val request = QuestionRequest(sid, tc, params.objs("questions").map { q ->
                    Question(q.str("question") ?: "", q.str("header"), q.bool("multi") ?: false,
                        q.objs("options").map { QuestionOption(it.str("label") ?: "", it.str("description")) })
                }, params, answered)
                ask(answered, "$sid/$tc", { h.onQuestion(request) }) { a -> result(mapOf("answers" to a)) }
            }
            // fs and terminal methods: the agent device does not use our files (chat spec §7)
            else -> CompletableFuture.completedFuture(error(RpcException.METHOD_NOT_FOUND, "method not found"))
        }
    }

    /** ask runs the application's handler; the request ends when it answers or another client did first. */
    private fun <T> ask(answered: CompletableFuture<Unit>, key: String, run: () -> java.util.concurrent.CompletionStage<T>, reply: (T) -> List<Any?>): CompletableFuture<List<Any?>> {
        val out = CompletableFuture<List<Any?>>()
        // another client answered first: our reply is not needed, but the stream must go on
        answered.thenRun { out.complete(error(RpcException.FAILED, "answered elsewhere")) }
        try {
            run().whenComplete { v, e ->
                pending.remove(key)
                if (e != null) out.complete(error(RpcException.FAILED, e.message ?: "cancelled"))
                else out.complete(reply(v))
            }
        } catch (e: Exception) {
            pending.remove(key)
            out.complete(error(RpcException.FAILED, e.message ?: "failed"))
        }
        return out
    }

    private fun result(r: Any?): List<Any?> = listOf(wire(mapOf("jsonrpc" to "2.0", "result" to r)))

    private fun error(code: Int, message: String): List<Any?> =
        listOf(wire(mapOf("jsonrpc" to "2.0", "error" to mapOf("code" to code, "message" to message))))

    companion object {
        /** A fresh message id for [prompt] and [newSession]. */
        fun newMid(): String = UUID.randomUUID().toString().replace("-", "").take(16)
    }
}
