package io.github.cristeaion.signalr

import kotlinx.serialization.json.JsonElement

/** Represents a server-to-client invocation event. */
data class SignalREvent(
    val target: String,
    val arguments: List<JsonElement>
)
