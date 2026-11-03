package io.github.p2pact

/** Levels of a grant (spec §8.1). */
const val LEVEL_READ = 1 // the methods a tool marks as read, and the tool's events
const val LEVEL_WRITE = 2 // everything

/** ALL_TOOLS is the tool name that stands for every tool, present and future. */
const val ALL_TOOLS = "*"

/** Grant lets a device use a tool at a level: one element of [[tool name, level]…] (spec §8.1). */
data class Grant(val tool: String, val level: Int)

/** defaultGrants is what a person's own device gets: everything, ["*", 2]. A trusted device whose grants were never set has it too. */
fun defaultGrants(): List<Grant> = listOf(Grant(ALL_TOOLS, LEVEL_WRITE))

/**
 * grantLevel is the level a list of grants gives for a tool name: the highest of the entries that
 * name it or "*"; 0 — none. A null list is the default (everything).
 */
fun grantLevel(grants: List<Grant>?, tool: String): Int {
    if (grants == null) return LEVEL_WRITE
    var level = 0
    for (g in grants) if ((g.tool == tool || g.tool == ALL_TOOLS) && g.level > level) level = g.level
    return level
}

/** allowed is what we let the device use of ours (spec §8.1); the default if never set. */
fun Peer.allowed(): List<Grant> = grants?.toList() ?: defaultGrants()

/** offered is what the device lets us use of its tools, as it told us; the default if it never did. */
fun Peer.offered(): List<Grant> = granted?.toList() ?: defaultGrants()

/** grantsWire is the grants as they go on the wire: [[tool name, level]…]. */
internal fun grantsWire(g: List<Grant>): List<Any?> = g.map { listOf(it.tool, it.level) }

/** parseGrants reads [[tool name, level]…]; a malformed entry is skipped. null — not a list. */
internal fun parseGrants(v: Any?): List<Grant>? {
    val list = v.asList() ?: return null
    val out = ArrayList<Grant>()
    for (e in list) {
        val pair = e.asList() ?: continue
        val tool = pair.getOrNull(0).asString() ?: continue
        val level = pair.getOrNull(1).asInt() ?: continue
        if (level < 1) continue
        out.add(Grant(tool, minOf(level, LEVEL_WRITE)))
    }
    return out
}

/** cleanGrants drops entries without a level and merges repeats, keeping the highest. */
internal fun cleanGrants(input: List<Grant>): List<Grant> {
    val out = ArrayList<Grant>()
    for (g0 in input) {
        if (g0.tool.isEmpty() || g0.level < 1) continue
        val g = if (g0.level > LEVEL_WRITE) g0.copy(level = LEVEL_WRITE) else g0
        val i = out.indexOfFirst { it.tool == g.tool }
        if (i >= 0) out[i] = out[i].copy(level = maxOf(out[i].level, g.level)) else out.add(g)
    }
    return out
}

internal fun grantsToJson(g: List<Grant>): List<Any?> = g.map { mapOf("tool" to it.tool, "level" to it.level) }

internal fun grantsFromJson(v: Any?): List<Grant>? {
    val l = v as? List<*> ?: return null
    return l.mapNotNull { e ->
        val m = e as? Map<*, *> ?: return@mapNotNull null
        Grant(m["tool"] as? String ?: return@mapNotNull null, (m["level"] as? Number)?.toInt() ?: return@mapNotNull null)
    }
}

/**
 * LevelHandler is a tool method that checks the caller's level itself: the node only requires
 * read, and passes the level (1 or 2) on. For a tool whose single method carries many (chat 1.1:
 * one P2PACT method for all of ACP), where what a call needs depends on its arguments.
 */
fun interface LevelHandler {
    fun handle(peer: String, level: Int, args: List<Any?>): List<Any?>
}

/**
 * ToolHandler is one handler for every method of a tool (see [Node.handleToolAll]): it gets the
 * method number and the caller's level (1 or 2), and checks the level itself.
 */
fun interface ToolHandler {
    fun handle(peer: String, level: Int, method: Int, args: List<Any?>): List<Any?>
}
