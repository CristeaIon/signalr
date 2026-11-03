package io.github.cristeaion.signalr

import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.json.JsonElement

/** A connection to a SignalR hub. Create one with [HubConnectionBuilder]. */
interface HubConnection {
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

    suspend fun connect()

    suspend fun disconnect()

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
