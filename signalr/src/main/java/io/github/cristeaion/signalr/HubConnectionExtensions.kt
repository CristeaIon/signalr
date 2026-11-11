package io.github.cristeaion.signalr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull

/** Decodes the argument at [index], or `null`/default handling if the server omitted it. */
public inline fun <reified T> List<JsonElement>.decodeArg(index: Int): T =
    decodeJsonElementSafe(getOrElse(index) { JsonNull })

public inline fun <reified T1> HubConnection.on(
    method: String,
    crossinline action: (T1) -> Unit
): Subscription = on(method) { args -> action(args.decodeArg(0)) }

public inline fun <reified T1, reified T2> HubConnection.on(
    method: String,
    crossinline action: (T1, T2) -> Unit
): Subscription = on(method) { args -> action(args.decodeArg(0), args.decodeArg(1)) }

public inline fun <reified T1, reified T2, reified T3> HubConnection.on(
    method: String,
    crossinline action: (T1, T2, T3) -> Unit
): Subscription = on(method) { args -> action(args.decodeArg(0), args.decodeArg(1), args.decodeArg(2)) }

/**
 * Typed result variant of [HubConnection.invoke]. The explicit type argument selects this over the
 * raw member, which is what the body below calls.
 */
public suspend inline fun <reified T> HubConnection.invoke(method: String, vararg args: Any?, timeoutMs: Long? = null): T {
    val result: JsonElement = invoke(method, *args, timeoutMs = timeoutMs)
    return decodeJsonElementSafe(result)
}

/** Collects [HubConnection.errorFlow] into [action] for as long as [scope] is active. */
public fun HubConnection.onError(scope: CoroutineScope, action: (Throwable) -> Unit): Job =
    scope.launch { errorFlow.collect(action) }

/** Runs [action] each time automatic reconnect restores the connection, for as long as [scope] is active. */
public fun HubConnection.onReconnected(scope: CoroutineScope, action: suspend () -> Unit): Job =
    scope.launch { reconnectedFlow.collect { action() } }

/** Type-safe Flow extension functions for HubConnection events. */
@JvmName("onFlowRaw")
public fun HubConnection.onFlow(method: String): Flow<List<JsonElement>> =
    eventsFor(method).map { event -> event.arguments }

@JvmName("onFlow1")
public inline fun <reified T1> HubConnection.onFlow(method: String): Flow<T1> =
    eventsFor(method).map { event -> event.arguments.decodeArg<T1>(0) }

@JvmName("onFlow2")
public inline fun <reified T1, reified T2> HubConnection.onFlow(method: String): Flow<Pair<T1, T2>> =
    eventsFor(method).map { event ->
        val args = event.arguments
        Pair(args.decodeArg<T1>(0), args.decodeArg<T2>(1))
    }

@JvmName("onFlow3")
public inline fun <reified T1, reified T2, reified T3> HubConnection.onFlow(method: String): Flow<Triple<T1, T2, T3>> =
    eventsFor(method).map { event ->
        val args = event.arguments
        Triple(args.decodeArg<T1>(0), args.decodeArg<T2>(1), args.decodeArg<T3>(2))
    }

/** Events whose target is [method], or a classic hub-qualified `<hub>.<method>`, case-insensitively. */
@PublishedApi
internal fun HubConnection.eventsFor(method: String): Flow<SignalREvent> {
    val targetLower = method.lowercase()
    return eventsFlow.filter { event ->
        val eventLower = event.target.lowercase()
        eventLower == targetLower || eventLower.endsWith(".$targetLower")
    }
}
