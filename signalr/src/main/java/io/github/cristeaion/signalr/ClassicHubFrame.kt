package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement

/** A classic ASP.NET SignalR (2.x) persistent-connection frame. */
@Serializable
internal data class ClassicHubFrame(
    val C: String? = null,
    val M: List<ClassicHubInvocation>? = null,
    val I: String? = null,
    val R: JsonElement? = null,
    val E: String? = null
)
