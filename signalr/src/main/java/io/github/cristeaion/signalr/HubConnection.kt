package io.github.cristeaion.signalr

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.json.JsonElement

/**
 * A connection to a SignalR hub. Create one with [HubConnectionBuilder].
 *
 * The connection owns an HTTP client. Call [close] once you are done with it for good: it
 * disconnects and releases the client. After [close], [connect] throws.
 */
interface HubConnection : AutoCloseable {
    val connectionState: StateFlow<HubConnectionState>

    /**
     * Errors that have no caller to be thrown at: transport failures, frames the server pushed as
     * errors, and exceptions escaping a handler registered with [on]. Failures of [connect],
     * [disconnect], [send] and [invoke] are thrown to their caller instead.
     */
    val errorFlow: SharedFlow<Throwable>

    /**
     * SharedFlow emitting all incoming server-to-client events.
     */
    val eventsFlow: SharedFlow<SignalREvent>

    /**
     * Emits each time automatic reconnect restores a dropped connection. The server sees a new
     * connection, so re-join groups and re-subscribe here. Handlers registered with [on] are kept
     * and need no re-registering. Never emits without [HubConnectionBuilder.withAutomaticReconnect].
     */
    val reconnectedFlow: SharedFlow<Unit>

    /**
     * Connects once. A failure is thrown and is not retried, even with automatic reconnect:
     * that only restores a connection that was established and then dropped.
     */
    suspend fun connect()

    /** Closes the session and stops any automatic reconnect. The connection can [connect] again afterwards. */
    suspend fun disconnect()

    /**
     * Disconnects and releases the underlying HTTP client. It does not block: the WebSocket is
     * closed gracefully in the background. Calling it again does nothing.
     */
    override fun close()

    /** Fire-and-forget hub call. */
    suspend fun send(method: String, vararg args: Any)

    /** Hub call that waits for the server's result. Use the reified overload for a typed result. */
    suspend fun invoke(method: String, vararg args: Any): JsonElement

    /**
     * Registers a handler for server-to-client calls to [method], matched case-insensitively.
     * Use the reified overloads to receive decoded arguments instead of raw JSON.
     */
    fun on(method: String, handler: (List<JsonElement>) -> Unit): Subscription
}
