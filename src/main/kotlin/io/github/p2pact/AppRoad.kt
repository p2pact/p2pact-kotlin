package io.github.p2pact

import java.io.IOException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * AppRoad is a road the application provides (spec §3): Bluetooth LE GATT, a serial line, a pipe
 * to another program — anything the node cannot open by itself. The node calls [send] from one
 * thread of its own, one call at a time, never on the node's thread: it may block until the link
 * has taken the bytes (a GATT write waits for its acknowledgement, a notification for its turn).
 * What arrives the application hands to [AttachedRoad.receive].
 */
fun interface AppRoad {
    /**
     * send carries one unit of at most [AppRoadConfig.mtu] bytes: a packet on a datagram road, a
     * piece of the byte stream on a stream road. An exception on a stream road detaches it: the
     * stream is broken mid-packet, so the application attaches the link again once it is back.
     */
    @Throws(IOException::class)
    fun send(b: ByteArray)
}

/**
 * AppRoadConfig describes an application road.
 *
 * - [name] — the kind of road, shown by [Node.paths] as "app:<name>" ("ble");
 * - [stream] — the link keeps order and loses nothing while it is up (GATT writes and
 *   notifications, a serial line, a pipe): packets are framed as on TCP — a 2-byte length, then
 *   the packet — and the frames are cut into pieces of at most [mtu] bytes, so any MTU works.
 *   Otherwise the road carries datagrams that may be lost or reordered, one packet each, and must
 *   take any packet whole: [mtu] at least [MAX_ROAD_PACKET];
 * - [mtu] — the largest unit [AppRoad.send] takes (a GATT link: the negotiated ATT MTU minus 3);
 * - [rate] — what the link carries, bytes per second, if the application knows it; 0 —
 *   [DEFAULT_APP_RATE] until the node has measured it. It sets how much the node keeps in flight
 *   on the road before the first measurement, so a slow link does not get a window meant for a
 *   fast one and its timers do not fire on its queue.
 */
data class AppRoadConfig(val name: String = "app", val stream: Boolean, val mtu: Int, val rate: Int = 0)

/** MAX_ROAD_PACKET is the largest packet a road must carry (the TCP road's frame takes the same). */
const val MAX_ROAD_PACKET = MAX_PACKET + 64

/** DEFAULT_APP_RATE is the rate assumed for an application road that declares none: Bluetooth LE carries tens of KB/s. */
const val DEFAULT_APP_RATE = 16 shl 10

/** APP_QUEUE: packets waiting for an application road's send; past it they are dropped, and delivery resends them. */
private const val APP_QUEUE = 256

private val appRoadSeq = AtomicInteger()

/** AttachedRoad is an application road attached to a node (see [Node.attachRoad]). */
class AttachedRoad internal constructor(
    private val app: AppRoad,
    val config: AppRoadConfig,
    /** the device it reaches, as the application told the node (null — learnt from packets) */
    val peer: String?,
    private val deliver: (ByteArray, Endpoint) -> Unit,
    private val onDetach: (AttachedRoad) -> Unit,
) : Road {
    override val name = "app:" + config.name
    internal val id = "#" + appRoadSeq.incrementAndGet()
    internal val endpoint = Endpoint(this, id)
    private val out = LinkedBlockingQueue<ByteArray>(APP_QUEUE)
    private val stopped = AtomicBoolean()
    private var buf = ByteArray(0) // the stream being reassembled
    private var have = 0

    init {
        Thread({ write() }, "p2pact-app-road").apply { isDaemon = true }.start()
    }

    /** rate is the link rate to plan with before one is measured, bytes/s. */
    internal val rate: Double get() = if (config.rate > 0) config.rate.toDouble() else DEFAULT_APP_RATE.toDouble()

    /** send queues a packet for the application's send (the node's [Road]; addr is the attachment's own). */
    override fun send(addr: String, pkt: ByteArray): Boolean {
        if (pkt.size > MAX_ROAD_PACKET || stopped.get()) return false
        val b = if (config.stream) {
            ByteArray(2 + pkt.size).also {
                it[0] = (pkt.size shr 8).toByte()
                it[1] = pkt.size.toByte()
                System.arraycopy(pkt, 0, it, 2, pkt.size)
            }
        } else pkt.copyOf()
        return out.offer(b)
    }

    /** write hands queued packets to the application, a stream road's in pieces of at most the MTU. */
    private fun write() {
        while (!stopped.get()) {
            val b = try { out.poll(1, TimeUnit.SECONDS) } catch (e: InterruptedException) { return } ?: continue
            if (stopped.get()) return
            if (!config.stream) {
                try { app.send(b) } catch (e: Exception) { }
                continue
            }
            var at = 0
            while (at < b.size) {
                val k = minOf(b.size - at, config.mtu)
                try {
                    app.send(b.copyOfRange(at, at + k))
                } catch (e: Exception) {
                    detach() // the stream is broken mid-packet
                    return
                }
                at += k
            }
        }
    }

    /**
     * receive hands the node what arrived on the road: a datagram, or the next piece of the byte
     * stream (pieces may be of any size). Call it from one thread at a time, in the order the
     * bytes came.
     */
    fun receive(b: ByteArray) {
        if (stopped.get()) return
        if (!config.stream) {
            if (b.size <= MAX_ROAD_PACKET) deliver(b.copyOf(), endpoint)
            return
        }
        synchronized(this) {
            if (have + b.size > buf.size) buf = buf.copyOf(maxOf(have + b.size, 2 * buf.size, 4096))
            System.arraycopy(b, 0, buf, have, b.size)
            have += b.size
            var at = 0
            while (have - at >= 2) {
                val k = ((buf[at].toInt() and 0xff) shl 8) or (buf[at + 1].toInt() and 0xff)
                if (k > MAX_ROAD_PACKET) { // not a frame: the stream is damaged, nothing tells where the next one starts
                    have = 0
                    return
                }
                if (have - at < 2 + k) break
                deliver(buf.copyOfRange(at + 2, at + 2 + k), endpoint)
                at += 2 + k
            }
            if (at > 0) {
                System.arraycopy(buf, at, buf, 0, have - at)
                have -= at
            }
        }
    }

    /** drop discards what waits for the application's send (the node pauses): delivery sends it again after resume. */
    internal fun drop() = out.clear()

    /** detach stops using the road: the link is gone. The session goes on over the other roads. */
    fun detach() {
        if (!stopped.compareAndSet(false, true)) return
        out.clear()
        onDetach(this)
    }

    val isDetached: Boolean get() = stopped.get()
}
