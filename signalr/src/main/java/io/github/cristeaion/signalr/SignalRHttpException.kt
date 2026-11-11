package io.github.cristeaion.signalr

import java.io.IOException

/**
 * An HTTP request made while connecting failed: negotiate, or classic `/start`. [statusCode]
 * is the HTTP status when the server answered with a non-2xx one, else `null`. Check it to
 * tell an expired token (401/403) from a server outage:
 *
 * ```kotlin
 * catch (e: SignalRHttpException) { if (e.statusCode == 401) auth.refresh() }
 * ```
 */
public open class SignalRHttpException(message: String, public val statusCode: Int? = null) : IOException(message) {
    /** True for 401 Unauthorized and 403 Forbidden: retrying with the same credentials won't help. */
    public val isAuthFailure: Boolean get() = statusCode == 401 || statusCode == 403
}

/**
 * Negotiate failed: a non-2xx status ([statusCode]), an `error` in the server's answer, a server
 * that doesn't offer WebSockets (the only transport this client supports), or too many redirects.
 */
public class NegotiateException(message: String, statusCode: Int? = null) : SignalRHttpException(message, statusCode)
