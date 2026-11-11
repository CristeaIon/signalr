package io.github.cristeaion.signalr

import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/** An ASP.NET Core SignalR JSON hub protocol message. */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
internal data class HubMessageJson(
    val type: Int = 0,
    val invocationId: String? = null,
    val target: String? = null,
    val arguments: List<JsonElement>? = null,
    val result: JsonElement? = null,
    val error: String? = null,
    /**
     * Close message (type 7) only: whether the server lets the client reconnect. Never sent:
     * the server would try to read a null here as a boolean.
     */
    @EncodeDefault(EncodeDefault.Mode.NEVER)
    val allowReconnect: Boolean? = null,
)
