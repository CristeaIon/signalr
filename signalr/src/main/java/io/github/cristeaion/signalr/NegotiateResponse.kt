package io.github.cristeaion.signalr

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

@Serializable
internal data class NegotiateResponse(
    val connectionId: String? = null,
    val connectionToken: String? = null,
    val negotiateVersion: Int? = null,
    val availableTransports: List<AvailableTransport>? = null,
    val error: String? = null,
    // Classic SignalR / alternative field names using @SerialName
    @SerialName("ConnectionId")
    val classicConnectionId: String? = null,
    @SerialName("ConnectionToken")
    val classicConnectionToken: String? = null,
    @SerialName("Url")
    val classicUrl: String? = null,
    @SerialName("ProtocolVersion")
    val protocolVersion: String? = null,
    @SerialName("TryWebSockets")
    val tryWebSockets: Boolean? = null,
    val accessToken: String? = null
) {
    fun getEffectiveConnectionToken(): String? {
        return connectionToken ?: classicConnectionToken ?: connectionId ?: classicConnectionId
    }

    fun getEffectiveConnectionId(): String? {
        return connectionId ?: classicConnectionId ?: connectionToken ?: classicConnectionToken
    }
}
