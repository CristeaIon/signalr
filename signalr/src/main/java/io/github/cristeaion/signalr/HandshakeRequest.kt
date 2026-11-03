package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable

@Serializable
internal data class HandshakeRequest(
    val protocol: String = "json",
    val version: Int = 1
)
