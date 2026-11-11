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
    /** Classic only: seconds without any frame after which the connection is dead; null = keep-alive off. */
    @SerialName("KeepAliveTimeout")
    val keepAliveTimeout: Double? = null,
    /**
     * Core only: negotiate redirect, e.g. Azure SignalR Service. Negotiate again at this URL,
     * using [accessToken]. Distinct from classic's capitalised "Url", which is just its path.
     */
    val url: String? = null,
    val accessToken: String? = null
) {
    fun getEffectiveConnectionToken(): String? {
        return connectionToken ?: classicConnectionToken ?: connectionId ?: classicConnectionId
    }

    fun getEffectiveConnectionId(): String? {
        return connectionId ?: classicConnectionId ?: connectionToken ?: classicConnectionToken
    }
}
