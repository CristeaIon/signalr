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
public interface HubConnection : AutoCloseable {
    public val connectionState: StateFlow<HubConnectionState>

    /**
     * Errors that have no caller to be thrown at: transport failures, frames the server pushed as
     * errors, and exceptions escaping a handler registered with [on]. Failures of [connect],
     * [disconnect], [send] and [invoke] are thrown to their caller instead.
     */
    public val errorFlow: SharedFlow<Throwable>

    /**
     * SharedFlow emitting all incoming server-to-client events.
     */
    public val eventsFlow: SharedFlow<SignalREvent>

    /**
     * Emits each time automatic reconnect restores a dropped connection. The server sees a new
     * connection, so re-join groups and re-subscribe here. Handlers registered with [on] are kept
     * and need no re-registering. Never emits without [HubConnectionBuilder.withAutomaticReconnect].
     */
    public val reconnectedFlow: SharedFlow<Unit>

    /**
     * Connects and returns once the state is CONNECTED. A failure is thrown and is not retried,
     * even with automatic reconnect: that only restores a connection that was established and
     * then dropped.
     *
     * Already CONNECTED: returns at once. CONNECTING or RECONNECTING: waits for that attempt and
     * returns when it connects, or throws why it didn't (its error, a [ReconnectFailedException],
     * or an `IllegalStateException` when [disconnect] or [close] interrupted it). Cancelling a
     * waiting call doesn't stop the attempt.
     */
    public suspend fun connect()

    /** Closes the session and stops any automatic reconnect. The connection can [connect] again afterwards. */
    public suspend fun disconnect()

    /**
     * Disconnects and releases the underlying HTTP client. It does not block: the WebSocket is
     * closed gracefully in the background. Calling it again does nothing.
     */
    override fun close()

    /**
     * Fire-and-forget hub call. Arguments may be `null`, primitives, `Map`s, collections, arrays,
     * `JsonElement`s or `@Serializable` objects; anything else throws [IllegalArgumentException].
     */
    public suspend fun send(method: String, vararg args: Any?)

    /**
     * Hub call that waits for the server's result. Use the reified overload for a typed result.
     *
     * [timeoutMs] limits the wait (pass it by name: `invoke("Get", id, timeoutMs = 5_000)`).
     * `null` uses [HubConnectionBuilder.withInvocationTimeout]; `0` waits as long as the
     * connection lasts. Throws [InvocationTimeoutException] when time runs out, and
     * [ConnectionLostException] when the connection drops or is disconnected first.
     */
    public suspend fun invoke(method: String, vararg args: Any?, timeoutMs: Long? = null): JsonElement

    /**
     * Registers a handler for server-to-client calls to [method], matched case-insensitively.
     * Use the reified overloads to receive decoded arguments instead of raw JSON.
     *
     * Handlers run on a background thread (never Main), one at a time and in the order the
     * server sent the calls. They don't hold up the connection: frames, pings and invoke results
     * keep arriving while a handler runs. Still keep them short, since a slow handler delays the
     * handlers after it. Hand long work to your own coroutine. Calls queued while
     * [Subscription.cancel] runs are skipped. An exception thrown by a handler goes to [errorFlow].
     */
    public fun on(method: String, handler: (List<JsonElement>) -> Unit): Subscription
}
