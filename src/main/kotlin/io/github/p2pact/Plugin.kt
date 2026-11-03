package io.github.p2pact

import java.io.File

/** Environment variables a device gives a program it starts (spec §2, local programs). */
const val ENV_SOCKET = "P2PACT_SOCKET" // the path of the device's local socket
const val ENV_INVITE = "P2PACT_INVITE" // a single-use invite with no hints

/** PluginSession is a plugin's node and the device it runs on. */
class PluginSession(val node: Node, val host: Peer)

/**
 * newPlugin is the plugin side (spec §2): it reads P2PACT_SOCKET and P2PACT_INVITE from env,
 * dials the socket and pairs with kind [Kind.PLUGIN] and no wants. dir keeps the plugin's keys; the
 * node is started. A plugin started again gets a new invite and pairs again. Needs Java 16+
 * (Unix domain sockets): see [localSocketsSupported].
 */
fun newPlugin(dir: File, name: String, timeoutMs: Long = 30_000, env: Map<String, String> = System.getenv()): PluginSession {
    val sock = env[ENV_SOCKET].orEmpty()
    val inv = env[ENV_INVITE].orEmpty()
    require(sock.isNotEmpty() && inv.isNotEmpty()) { "$ENV_SOCKET and $ENV_INVITE are not set: not started by a P2PACT device" }
    val invite = Invite.parse(inv)
    val n = Node(dir, name)
    try {
        n.kind = Kind.PLUGIN
        n.useLocal(sock)
        n.start()
        val host = n.join(invite, timeoutMs)
        return PluginSession(n, host)
    } catch (e: Throwable) {
        n.close()
        throw e
    }
}
