package io.github.p2pact.client

import java.util.concurrent.CompletableFuture

// Types of tool 1 `p2pact.chat` (chat spec): ACP with `_meta.p2pact`. Unknown fields are ignored;
// every type keeps the JSON it was read from in `raw`.

class AgentInfo(val id: String, val name: String, val version: String?, val raw: JsonMap) {
    companion object {
        fun parse(m: JsonMap) = AgentInfo(m.str("id") ?: "", m.str("name") ?: m.str("id") ?: "", m.str("version"), m)
    }
}

/** The answer to `initialize`: what the computer's chat offers. */
class InitializeResult(
    val protocolVersion: Int,
    val agentName: String?,
    val agentTitle: String?,
    val agentVersion: String?,
    /** `session/load` works. */
    val loadSession: Boolean,
    /** The agents the computer can start (`_meta.p2pact.agents`), the first is the default. */
    val agents: List<AgentInfo>,
    /** The computer speaks the `_p2pact/<name>` extensions. */
    val extensions: Boolean,
    val raw: JsonMap,
    /** The extension version the computer speaks (`_meta.p2pact.v`); 2 and up: typed, complete results. 0 when absent. */
    val version: Int = 0,
) {
    companion object {
        fun parse(m: JsonMap): InitializeResult {
            val info = m.obj("agentInfo")
            val meta = m.p2pactMeta()
            return InitializeResult(
                m.int("protocolVersion") ?: 1, info?.str("name"), info?.str("title"), info?.str("version"),
                m.obj("agentCapabilities")?.bool("loadSession") ?: false,
                meta.objs("agents").map { AgentInfo.parse(it) }, (meta.int("ext") ?: 0) >= 1, m, meta.int("v") ?: 0,
            )
        }
    }
}

enum class SessionState(val wire: String) {
    WORKING("working"),

    /** Waits for the person. */
    WAITING("waiting"),
    IDLE("idle"),
    CLOSED("closed"),
    UNKNOWN("");

    companion object {
        fun of(s: String?) = entries.firstOrNull { it.wire == s && s != "" } ?: UNKNOWN
    }
}

/** A chat of a computer: an entry of `session/list` or `_p2pact/session_changed`. */
data class ChatSession(
    val id: String,
    val cwd: String?,
    val title: String?,
    val agent: String?,
    val state: SessionState,
    /** RFC 3339. */
    val updatedAt: String?,
    val raw: JsonMap,
) {
    companion object {
        fun parse(m: JsonMap): ChatSession {
            val meta = m.p2pactMeta()
            return ChatSession(
                m.str("sessionId") ?: "", m.str("cwd"), meta.str("title") ?: m.str("title"), meta.str("agent"),
                SessionState.of(meta.str("state")), meta.str("updatedAt") ?: m.str("updatedAt"), m,
            )
        }
    }
}

class SessionList(val sessions: List<ChatSession>, val nextCursor: String?, val raw: JsonMap)

/** An ACP content block: text, image, resource_link… */
class ContentBlock(val type: String, val text: String?, val raw: JsonMap) {
    companion object {
        fun parse(m: JsonMap?) = ContentBlock(m?.str("type") ?: "", m?.str("text"), m ?: emptyMap())

        fun text(text: String): JsonMap = mapOf("type" to "text", "text" to text)
        fun image(mimeType: String, base64: String): JsonMap = mapOf("type" to "image", "mimeType" to mimeType, "data" to base64)
    }
}

/** A tool call of an agent (`tool_call`, `tool_call_update` and the `toolCall` of a permission request). */
class ToolCallInfo(
    val toolCallId: String,
    val title: String?,
    /** ACP's kind: execute, read, edit… */
    val kind: String?,
    /** pending, in_progress, completed, failed. */
    val status: String?,
    val raw: JsonMap,
) {
    companion object {
        fun parse(m: JsonMap) = ToolCallInfo(m.str("toolCallId") ?: "", m.str("title"), m.str("kind"), m.str("status"), m)
    }
}

class PlanEntry(val content: String, val priority: String?, val status: String?, val raw: JsonMap)

/**
 * ChatUpdate is a `session/update`: a live message of a chat or a replayed one (`session/load`).
 * [seq] numbers the messages of a session; a client that loads a session again skips what it has
 * shown (chat spec §8). The pieces of a streamed agent message carry no `seq` but the first.
 */
sealed class ChatUpdate {
    abstract val sessionId: String
    abstract val seq: Long?

    /** RFC 3339. */
    abstract val time: String?

    /** The `update` object. */
    abstract val raw: JsonMap

    class UserMessage(override val sessionId: String, val content: ContentBlock, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate() {
        val text: String get() = content.text ?: ""
    }

    class AgentMessage(override val sessionId: String, val content: ContentBlock, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate() {
        val text: String get() = content.text ?: ""
    }

    class AgentThought(override val sessionId: String, val content: ContentBlock, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate() {
        val text: String get() = content.text ?: ""
    }

    class ToolCall(override val sessionId: String, val call: ToolCallInfo, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate()
    class ToolCallUpdate(override val sessionId: String, val call: ToolCallInfo, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate()
    class Plan(override val sessionId: String, val entries: List<PlanEntry>, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate()
    class ModeChanged(override val sessionId: String, val modeId: String, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate()

    /** An update this SDK has no type for: read [raw]. */
    class Other(override val sessionId: String, val type: String, override val seq: Long?, override val time: String?, override val raw: JsonMap) : ChatUpdate()

    companion object {
        fun parse(sessionId: String, u: JsonMap): ChatUpdate {
            val meta = u.p2pactMeta()
            val seq = meta.long("seq")
            val time = meta.str("time")
            return when (val type = u.str("sessionUpdate") ?: "") {
                "user_message_chunk" -> UserMessage(sessionId, ContentBlock.parse(u.obj("content")), seq, time, u)
                "agent_message_chunk" -> AgentMessage(sessionId, ContentBlock.parse(u.obj("content")), seq, time, u)
                "agent_thought_chunk" -> AgentThought(sessionId, ContentBlock.parse(u.obj("content")), seq, time, u)
                "tool_call" -> ToolCall(sessionId, ToolCallInfo.parse(u), seq, time, u)
                "tool_call_update" -> ToolCallUpdate(sessionId, ToolCallInfo.parse(u), seq, time, u)
                "plan" -> Plan(sessionId, u.objs("entries").map { PlanEntry(it.str("content") ?: "", it.str("priority"), it.str("status"), it) }, seq, time, u)
                "current_mode_update" -> ModeChanged(sessionId, u.str("currentModeId") ?: "", seq, time, u)
                else -> Other(sessionId, type, seq, time, u)
            }
        }
    }
}

class NewSession(val sessionId: String, val raw: JsonMap)

/** What `session/load` gave: the replayed [updates] (all of them have come when it returns). */
class LoadedSession(val sessionId: String, val updates: List<ChatUpdate>, val older: Boolean, val raw: JsonMap)

class PromptResult(val stopReason: String?, /** The `mid` was already executed; nothing was sent again. */ val duplicate: Boolean, val raw: JsonMap)

/** A message of a conversation, as `_p2pact/older` gives it. */
class ChatMessage(val seq: Long, val role: String, val text: String, val time: String?, val images: List<String>, val raw: JsonMap)

class OlderPage(val sessionId: String, val messages: List<ChatMessage>, val more: Boolean, val raw: JsonMap, /** The agent run the page belongs to, when the engine says. */ val run: String? = null)

class DiffFile(val path: String, val added: Int, val removed: Int, val status: String?)

class DiffResult(val files: List<DiffFile>, val patch: String, val truncated: Boolean, /** `not_found`, `not_git` or text. */ val error: String?, val raw: JsonMap)

class ImageResult(/** Base64. */ val data: String?, /** `not_found`, `no_file`, `too_big`, `bad_image`. */ val error: String?, val raw: JsonMap, val path: String? = null, val mime: String? = null)

/** `_p2pact/request_answered`: another client answered; stop showing the request. */
class RequestAnswered(val sessionId: String, val toolCallId: String)

// --- the computer asks, the application answers ---

class PermissionOption(val optionId: String, val name: String, /** allow_once, allow_always, reject_once, reject_always. */ val kind: String?, val description: String?, val raw: JsonMap)

/** The outcome of a permission request. */
sealed class PermissionOutcome {
    class Selected(val optionId: String) : PermissionOutcome()
    object Cancelled : PermissionOutcome()
}

/**
 * PermissionRequest is `session/request_permission` (chat spec §6): the agent asks the person
 * to allow something. It is also how an agent asks a single question: [toolCall] `.title` is the
 * question and the options are the answers (§9.1).
 */
class PermissionRequest(
    val sessionId: String,
    val toolCall: ToolCallInfo,
    val options: List<PermissionOption>,
    val raw: JsonMap,
    /** Completes when another client answered first ([RequestAnswered]): stop showing the request. */
    val answered: CompletableFuture<Unit>,
) {
    /** The first option that allows, once before always; Cancelled if there is none. */
    fun allow(): PermissionOutcome = pick("allow_once", "allow_always")

    /** The first option that rejects. */
    fun deny(): PermissionOutcome = pick("reject_once", "reject_always")

    fun select(option: PermissionOption): PermissionOutcome = PermissionOutcome.Selected(option.optionId)

    private fun pick(vararg kinds: String): PermissionOutcome {
        for (k in kinds) options.firstOrNull { it.kind == k }?.let { return select(it) }
        return PermissionOutcome.Cancelled
    }
}

class QuestionOption(val label: String, val description: String?)

class Question(val question: String, val header: String?, /** The answer may take several options. */ val multi: Boolean, val options: List<QuestionOption>)

/** QuestionRequest is `_p2pact/question`: several questions, or answers that may take several options (§9.1). */
class QuestionRequest(
    val sessionId: String,
    val toolCallId: String,
    val questions: List<Question>,
    val raw: JsonMap,
    val answered: CompletableFuture<Unit>,
) {
    companion object {
        /** The answer to a `multi` question: the labels joined by ", ". */
        fun join(labels: List<String>) = labels.joinToString(", ")
    }
}

/** The application's answer to permission requests. Return a future that completes when the person decided. */
fun interface PermissionHandler {
    fun onPermission(request: PermissionRequest): java.util.concurrent.CompletionStage<PermissionOutcome>
}

/**
 * The application's answer to questions: {question text → label} (see [QuestionRequest.join]);
 * a future that fails answers with an error (the question stays open on the computer).
 */
fun interface QuestionHandler {
    fun onQuestion(request: QuestionRequest): java.util.concurrent.CompletionStage<Map<String, String>>
}

// --- typed results of the `_p2pact/*` methods (chat spec §10) ---

/** The answer of `_p2pact/close`, `restart`, `hide`, `history_open`: [ok], or [error] (an `alert` line from an older engine). */
class ActionResult(val ok: Boolean, val error: String?, /** The old phone-line shape. */ val lines: List<JsonMap>, val raw: JsonMap)

class SlashArg(val value: String, val desc: String?, val raw: JsonMap)

/** A slash command of the agent (`_p2pact/commands`). */
class SlashCommand(val name: String, val desc: String?, val hint: String?, val args: List<SlashArg>, val raw: JsonMap)

/** `_p2pact/files`: the files of the chat's folder that match [query]. */
class FilesResult(val files: List<String>, val query: String, val raw: JsonMap)

/** A past conversation of an agent (`_p2pact/history`); [updated] is Unix milliseconds. */
class HistoryItem(val id: String, val agent: String, val dir: String, val title: String, val updated: Long, val count: Int, val raw: JsonMap)

/** `_p2pact/history`: one page of past conversations; [cursor] is the next offset when there is more. */
class HistoryPage(
    val items: List<HistoryItem>,
    val query: String,
    val from: Int,
    val total: Int,
    val cursor: Int?,
    /** Some agents could not be read in time. */
    val partial: Boolean,
    val raw: JsonMap,
)

/** `_p2pact/projects`: the folders the computer knows. */
class ProjectsResult(
    val dirs: List<String>,
    /** Project name -> its folders. */
    val projectDirs: Map<String, List<String>>,
    /** Folder -> git remote. */
    val dirRepos: Map<String, String>,
    val raw: JsonMap,
)

/** An agent of `_p2pact/agents`. */
class AgentEntry(val id: String, val name: String, val type: String?, val enabled: Boolean, val models: List<String>, val default: String?, val raw: JsonMap)

class AgentPreset(val id: String, val name: String, val raw: JsonMap)

class AgentsResult(val agents: List<AgentEntry>, val presets: List<AgentPreset>, val raw: JsonMap)
