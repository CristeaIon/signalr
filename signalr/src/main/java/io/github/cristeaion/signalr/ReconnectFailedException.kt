package io.github.cristeaion.signalr

import java.io.IOException

/**
 * Emitted on [HubConnection.errorFlow] when automatic reconnect gives up because its
 * [RetryPolicy] returned `null`. The state is then DISCONNECTED; call `connect()` to try again.
 */
class ReconnectFailedException(val attempts: Int, cause: Throwable?) :
    IOException("Reconnect gave up after $attempts attempt(s).", cause)
