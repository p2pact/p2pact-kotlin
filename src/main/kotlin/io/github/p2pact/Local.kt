package io.github.p2pact

import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.ProtocolFamily
import java.net.SocketAddress
import java.net.StandardProtocolFamily
import java.nio.ByteBuffer
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.PosixFilePermissions
import java.util.concurrent.atomic.AtomicLong

/*
 * The local socket road (spec §3): programs on the same computer (a plugin, a CLI, an MCP server)
 * talk to the node with the same packets over a stream socket, framed as on TCP. The node
 * listens ([Node.listenLocal]); a program dials ([Node.useLocal], then join or a session as
 * usual). Only the device's own OS user may open the socket: that is who may even try to pair;
 * trust still comes from pairing.
 *
 * Unix domain sockets in Java are a Java 16 feature (UnixDomainSocketAddress, SocketChannel
 * with StandardProtocolFamily.UNIX). Android's class library has none of it, so everything of it
 * is reached by reflection, only when the road is asked for: the rest of the library loads and
 * runs on Android untouched, and listenLocal / useLocal there fail with UnsupportedOperationException.
 */

/** localSocketsSupported reports whether this runtime has Unix domain sockets (Java 16+; not Android). */
fun localSocketsSupported(): Boolean = try {
    LocalApi.family; LocalApi.address("/"); true
} catch (e: Throwable) {
    false
}

/** LocalApi is the Java 16 API reached by reflection. */
private object LocalApi {
    val family: ProtocolFamily by lazy {
        try {
            StandardProtocolFamily.valueOf("UNIX")
        } catch (e: IllegalArgumentException) {
            throw unsupported(e)
        }
    }
    private val of by lazy {
        try { Class.forName("java.net.UnixDomainSocketAddress").getMethod("of", String::class.java) } catch (e: Exception) { throw unsupported(e) }
    }
    private val openClient by lazy {
        try { SocketChannel::class.java.getMethod("open", ProtocolFamily::class.java) } catch (e: Exception) { throw unsupported(e) }
    }
    private val openServer by lazy {
        try { ServerSocketChannel::class.java.getMethod("open", ProtocolFamily::class.java) } catch (e: Exception) { throw unsupported(e) }
    }

    private fun unsupported(e: Throwable) = UnsupportedOperationException("this runtime has no Unix domain sockets (Java 16+ needed)", e)

    fun address(path: String): SocketAddress = of.invoke(null, path) as SocketAddress
    fun client(): SocketChannel = openClient.invoke(null, family) as SocketChannel
    fun server(): ServerSocketChannel = openServer.invoke(null, family) as ServerSocketChannel
}

/** LocalRoad is the local socket road: a stream road over a Unix domain socket. */
internal class LocalRoad private constructor(recv: Receiver, listenPath: String?) : StreamRoad("local", recv) {
    private val server: ServerSocketChannel?
    private val path = listenPath
    private val seq = AtomicLong()

    init {
        server = if (listenPath == null) null else {
            val ch = LocalApi.server()
            try {
                ch.bind(LocalApi.address(listenPath))
                restrict(listenPath)
            } catch (e: Exception) {
                try { ch.close() } catch (x: IOException) { }
                throw e
            }
            Thread({
                while (!closed.get()) {
                    val c = try { ch.accept() } catch (e: IOException) { break }
                    if (paused) {
                        try { c.close() } catch (e: IOException) { }
                        continue
                    }
                    // peers of a unix socket have no address: number them
                    accepted("local-" + seq.incrementAndGet(), link(c))
                }
            }, "p2pact-local-accept").apply { isDaemon = true }.start()
            ch
        }
    }

    override fun connect(addr: String): Link? {
        val ch = LocalApi.client()
        try {
            ch.connect(LocalApi.address(addr))
        } catch (e: IOException) {
            try { ch.close() } catch (x: IOException) { }
            return null
        }
        return link(ch)
    }

    override fun close() {
        closed.set(true)
        try { server?.close() } catch (e: IOException) { }
        if (path != null) try { Files.deleteIfExists(Paths.get(path)) } catch (e: IOException) { }
        super.close()
    }

    companion object {
        /** listen opens the socket at path (mode 0600). An old socket file nobody listens on is replaced. */
        fun listen(path: String, recv: Receiver): LocalRoad {
            require(path.isNotEmpty()) { "no socket path" }
            val p = Paths.get(path)
            if (Files.exists(p, LinkOption.NOFOLLOW_LINKS)) {
                val attrs = Files.readAttributes(p, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
                if (!attrs.isOther) throw IOException("$path is not a socket")
                val probe = LocalApi.client()
                val alive = try { probe.connect(LocalApi.address(path)); true } catch (e: IOException) { false } finally {
                    try { probe.close() } catch (e: IOException) { }
                }
                if (alive) throw IOException("$path is in use")
                Files.deleteIfExists(p)
            }
            return LocalRoad(recv, path)
        }

        /** dialer makes a road that dials only: the node does not listen. */
        fun dialer(recv: Receiver): LocalRoad {
            LocalApi.family // fails early where there are no Unix sockets
            return LocalRoad(recv, null)
        }

        /** restrict leaves the socket to our OS user only. */
        private fun restrict(path: String) {
            try {
                Files.setPosixFilePermissions(Paths.get(path), PosixFilePermissions.fromString("rw-------"))
            } catch (e: UnsupportedOperationException) {
                // not a POSIX file system (Windows): the socket belongs in a directory only the user can read
            }
        }

        /** link wraps a channel: reads and writes without the channel's blocking lock, so one can wait while the other goes. */
        private fun link(ch: SocketChannel): Link {
            val input = object : InputStream() {
                override fun read(): Int {
                    val b = ByteArray(1)
                    return if (read(b, 0, 1) < 0) -1 else b[0].toInt() and 0xff
                }

                override fun read(b: ByteArray, off: Int, len: Int): Int {
                    if (len == 0) return 0
                    return ch.read(ByteBuffer.wrap(b, off, len))
                }
            }
            val output = object : OutputStream() {
                override fun write(b: Int) = write(byteArrayOf(b.toByte()), 0, 1)

                override fun write(b: ByteArray, off: Int, len: Int) {
                    val bb = ByteBuffer.wrap(b, off, len)
                    while (bb.hasRemaining()) ch.write(bb)
                }
            }
            return Link(input.buffered(64 shl 10), output) { ch.close() }
        }
    }
}
