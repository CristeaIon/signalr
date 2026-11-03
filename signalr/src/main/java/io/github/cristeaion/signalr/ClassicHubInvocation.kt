package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/** A single hub call inside a classic ASP.NET SignalR frame's `M` array. */
@Serializable
internal data class ClassicHubInvocation(
    val H: String? = null,
    val M: String? = null,
    val A: List<JsonElement>? = null,
    val I: String? = null,
    val R: JsonElement? = null,
    val E: String? = null
)
