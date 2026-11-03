package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable

@Serializable
internal data class HandshakeResponse(
    val error: String? = null
)
