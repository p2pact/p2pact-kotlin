package io.github.p2pact

/**
 * Tools given by the spec (§8): the same number on every device. Their names are what grants
 * (§8.1) are written in.
 */
internal val FIXED_TOOLS = mapOf(
    "p2pact.service" to 0,
    "p2pact.chat" to 1,
    "p2pact.fleet" to 2,
    "p2pact.family" to 3,
    "p2pact.plugins" to 4,
    "p2pact.term" to 5,
)

/** fixedTool is the number the spec gives a tool name (§8): 0-63, the same on every device; null — the name is not one of the spec's. */
fun fixedTool(name: String): Int? = FIXED_TOOLS[name]

/** FIRST_NAMED_TOOL is the first number a device may choose for a named tool (spec §8). */
const val FIRST_NAMED_TOOL = 64

/**
 * validToolName reports a tool name of spec §8: lowercase, dot-separated, the reverse of a
 * domain ("com.example.jira"). Names under "p2pact." are given by the spec.
 */
fun validToolName(name: String): Boolean {
    val parts = name.split('.')
    if (parts.size < 2 || parts[0] == "p2pact") return false
    return parts.all { p ->
        p.isNotEmpty() && p.first() != '-' && p.last() != '-' && p.all { it in 'a'..'z' || it in '0'..'9' || it == '-' }
    }
}

/** ToolRef is a named tool on a device: its number there and its version. */
internal data class ToolRef(val number: Int, val version: Int)
