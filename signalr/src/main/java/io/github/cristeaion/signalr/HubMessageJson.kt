package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/** An ASP.NET Core SignalR JSON hub protocol message. */
@Serializable
internal data class HubMessageJson(
    val type: Int = 0,
    val invocationId: String? = null,
    val target: String? = null,
    val arguments: List<JsonElement>? = null,
    val result: JsonElement? = null,
    val error: String? = null
)
