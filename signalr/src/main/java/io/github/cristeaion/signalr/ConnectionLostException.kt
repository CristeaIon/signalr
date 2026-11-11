package io.github.cristeaion.signalr

import java.io.IOException

/**
 * Thrown from [HubConnection.invoke] when the connection goes away before the server's result
 * arrives: it dropped (the [cause] says why, if known), or [HubConnection.disconnect] /
 * [HubConnection.close] was called. The server may or may not have run the method.
 */
public class ConnectionLostException(message: String, cause: Throwable? = null) : IOException(message, cause)
