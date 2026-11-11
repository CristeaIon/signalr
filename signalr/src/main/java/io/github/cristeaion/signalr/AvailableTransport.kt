package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable

@Serializable
internal data class AvailableTransport(
    val transport: String,
    val transferFormats: List<String> = emptyList(),
)
