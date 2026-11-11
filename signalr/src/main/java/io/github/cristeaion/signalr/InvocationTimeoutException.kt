package io.github.cristeaion.signalr

import java.io.IOException

/**
 * Thrown from [HubConnection.invoke] when no result arrived within its timeout. The connection
 * stays up, and a result that arrives later is ignored. The server may still run the method.
 */
class InvocationTimeoutException(val method: String, val timeoutMs: Long) :
    IOException("Invocation '$method' got no result within ${timeoutMs}ms.")
