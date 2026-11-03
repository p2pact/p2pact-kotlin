package io.github.p2pact.client

import io.github.p2pact.Errors
import io.github.p2pact.P2pactError
import java.util.concurrent.CompletionException
import java.util.concurrent.ExecutionException

/**
 * A P2PACT error (core spec §6): the call never reached the tool. [code] is one of [Errors];
 * the subclasses name the ones an application handles.
 */
open class P2pactException(val code: Int, message: String) : RuntimeException(message)

/** Error 1: the device has no such tool or method. */
class NoMethodException(message: String) : P2pactException(Errors.NO_METHOD, message)

/** Error 2: the arguments are wrong. */
class BadArgsException(message: String) : P2pactException(Errors.BAD_ARGS, message)

/** Error 3: no such thing. */
class NotFoundException(message: String) : P2pactException(Errors.NOT_FOUND, message)

/** Error 4: the grants of the device do not allow the call (spec §8.1). */
class ForbiddenException(message: String) : P2pactException(Errors.FORBIDDEN, message)

/** Error 5: the call failed on the device. */
class FailedException(message: String) : P2pactException(Errors.FAILED, message)

/** Error 6: not delivered, e.g. a hosted tool whose plugin is gone (spec §8.2). */
class NotDeliveredException(message: String) : P2pactException(Errors.NOT_DELIVERED, message)

/**
 * A JSON-RPC error of the tool itself (the call was delivered): [code] is JSON-RPC's (`-32601` no
 * such method, `-32602` params, `-32004` no such resource, `-32000` failed), [data] the optional
 * extra.
 */
class RpcException(val code: Int, message: String, val data: Any? = null) : RuntimeException(message) {
    val isMethodNotFound get() = code == METHOD_NOT_FOUND
    val isNoSuchResource get() = code == NO_SUCH_RESOURCE

    companion object {
        const val METHOD_NOT_FOUND = -32601
        const val INVALID_PARAMS = -32602
        const val NO_SUCH_RESOURCE = -32004
        const val FAILED = -32000
    }
}

/** typed turns the library's [P2pactError] into the exception of its code; others stay as they are. */
internal fun typed(t: Throwable): Throwable {
    val e = if ((t is CompletionException || t is ExecutionException) && t.cause != null) t.cause!! else t
    if (e !is P2pactError) return e
    val m = e.message ?: ""
    return when (e.code) {
        Errors.NO_METHOD -> NoMethodException(m)
        Errors.BAD_ARGS -> BadArgsException(m)
        Errors.NOT_FOUND -> NotFoundException(m)
        Errors.FORBIDDEN -> ForbiddenException(m)
        Errors.FAILED -> FailedException(m)
        Errors.NOT_DELIVERED -> NotDeliveredException(m)
        else -> P2pactException(e.code, m)
    }
}
