package io.github.cristeaion.signalr

import java.io.IOException

/** Emitted on [HubConnection.errorFlow] when the server went silent for longer than the server timeout. */
public class ServerTimeoutException(public val timeoutMs: Long) :
    IOException("Server timeout elapsed: no message received for ${timeoutMs}ms. The connection was closed.")
