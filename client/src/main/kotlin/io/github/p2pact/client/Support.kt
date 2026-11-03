package io.github.p2pact.client

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit

/** A JSON object as the library reads it: keys are strings, numbers are Long or Double. */
typealias JsonMap = Map<String, Any?>

/** Subscription ends a subscription to [Events]. */
fun interface Subscription : AutoCloseable {
    override fun close()
}

/**
 * Events is a stream of values the application listens to. Listeners run on the library's
 * worker threads, one at a time per stream of a computer, in the order sent; a listener that
 * throws is skipped. The application moves to its own thread (main, a coroutine) itself.
 */
class Events<T> internal constructor() {
    private val listeners = CopyOnWriteArrayList<(T) -> Unit>()

    fun subscribe(listener: (T) -> Unit): Subscription {
        listeners.add(listener)
        return Subscription { listeners.remove(listener) }
    }

    internal fun emit(v: T) {
        for (l in listeners) {
            try {
                l(v)
            } catch (_: Exception) {
            }
        }
    }
}

/** await waits for a future and throws its failure itself, not wrapped. */
fun <T> CompletableFuture<T>.await(timeoutMs: Long = 30_000): T = try {
    get(timeoutMs, TimeUnit.MILLISECONDS)
} catch (e: ExecutionException) {
    throw e.cause ?: e
}

// --- reading JSON trees ---

fun Any?.asMap(): JsonMap? {
    val m = this as? Map<*, *> ?: return null
    @Suppress("UNCHECKED_CAST")
    return m as JsonMap
}

fun Any?.asList(): List<Any?>? = this as? List<Any?>

fun JsonMap.str(key: String): String? = this[key] as? String
fun JsonMap.int(key: String): Int? = (this[key] as? Number)?.toInt()
fun JsonMap.long(key: String): Long? = (this[key] as? Number)?.toLong()
fun JsonMap.bool(key: String): Boolean? = this[key] as? Boolean
fun JsonMap.obj(key: String): JsonMap? = this[key].asMap()
fun JsonMap.objs(key: String): List<JsonMap> = this[key].asList()?.mapNotNull { it.asMap() } ?: emptyList()

/** The object at `_meta.p2pact` of an ACP object (chat spec §8). */
fun JsonMap.p2pactMeta(): JsonMap = obj("_meta")?.obj("p2pact") ?: emptyMap()

/**
 * normalize makes a decoded CBOR value into a JSON tree: byte strings become base64 text, map
 * keys strings.
 */
internal fun normalize(v: Any?): Any? = when (v) {
    is Map<*, *> -> LinkedHashMap<String, Any?>().also { m -> v.forEach { (k, x) -> m[k.toString()] = normalize(x) } }
    is List<*> -> v.map { normalize(it) }
    is ByteArray -> java.util.Base64.getEncoder().encodeToString(v)
    else -> v
}

/** wire makes a JSON tree ready for CBOR: whole doubles become integers. */
internal fun wire(v: Any?): Any? = when (v) {
    is Map<*, *> -> LinkedHashMap<String, Any?>().also { m -> v.forEach { (k, x) -> m[k.toString()] = wire(x) } }
    is List<*> -> v.map { wire(it) }
    is Int, is Short, is Byte -> (v as Number).toLong()
    is Float -> v.toDouble()
    is Double -> if (v == Math.floor(v) && !v.isInfinite() && Math.abs(v) < 1e15) v.toLong() else v
    else -> v
}

internal fun <T> failed(e: Throwable): CompletableFuture<T> = CompletableFuture<T>().also { it.completeExceptionally(e) }

/** map is thenApply whose failure keeps its own type (no CompletionException) and is typed ([typed]). */
internal fun <A, B> CompletableFuture<A>.map(f: (A) -> B): CompletableFuture<B> {
    val out = CompletableFuture<B>()
    whenComplete { r, e ->
        if (e != null) out.completeExceptionally(typed(e))
        else try { out.complete(f(r)) } catch (x: Throwable) { out.completeExceptionally(typed(x)) }
    }
    return out
}

/** flatMap is thenCompose with the same error rules as [map]. */
internal fun <A, B> CompletableFuture<A>.flatMap(f: (A) -> CompletableFuture<B>): CompletableFuture<B> {
    val out = CompletableFuture<B>()
    whenComplete { r, e ->
        if (e != null) out.completeExceptionally(typed(e))
        else try {
            f(r).whenComplete { r2, e2 -> if (e2 != null) out.completeExceptionally(typed(e2)) else out.complete(r2) }
        } catch (x: Throwable) { out.completeExceptionally(typed(x)) }
    }
    return out
}

private val timer = java.util.concurrent.ScheduledThreadPoolExecutor(1) { r -> Thread(r, "p2pact-client-timer").apply { isDaemon = true } }
    .also { it.removeOnCancelPolicy = true }

/** settled completes when f completes (either way) or after ms, whichever is first (no Java 9 APIs: Android). */
internal fun settled(f: CompletableFuture<*>, ms: Long): CompletableFuture<Unit> {
    val out = CompletableFuture<Unit>()
    val t = timer.schedule({ out.complete(Unit) }, ms, TimeUnit.MILLISECONDS)
    f.whenComplete { _, _ -> t.cancel(false); out.complete(Unit) }
    return out
}
