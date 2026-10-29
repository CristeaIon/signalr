package io.github.cristeaion.signalr

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.serializer

@Serializable
data class HandshakeRequest(
    val protocol: String = "json",
    val version: Int = 1
)

@Serializable
data class HandshakeResponse(
    val error: String? = null
)

@Serializable
data class HubMessageJson(
    val type: Int = 0,
    val invocationId: String? = null,
    val target: String? = null,
    val arguments: List<JsonElement>? = null,
    val result: JsonElement? = null,
    val error: String? = null
)

@Serializable
data class ClassicHubInvocation(
    val H: String? = null,
    val M: String? = null,
    val A: List<JsonElement>? = null,
    val I: String? = null,
    val R: JsonElement? = null,
    val E: String? = null
)

@Serializable
data class ClassicHubFrame(
    val C: String? = null,
    val M: List<ClassicHubInvocation>? = null,
    val I: String? = null,
    val R: JsonElement? = null,
    val E: String? = null
)

@Serializable
data class NegotiateResponse(
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

@Serializable
data class AvailableTransport(
    val transport: String,
    val transferFormats: List<String>
)

object SignalRProtocol {
    const val RECORD_SEPARATOR = 0x1e.toChar()
    val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
        isLenient = true
    }

    fun createHandshakeMessage(): String {
        return json.encodeToString(HandshakeRequest.serializer(), HandshakeRequest()) + RECORD_SEPARATOR
    }

    fun parseFrames(payload: String): List<String> {
        if (payload.isEmpty()) return emptyList()
        return payload.split(RECORD_SEPARATOR).filter { it.isNotEmpty() }
    }

    fun anyToJsonElement(value: Any?): JsonElement {
        return when (value) {
            null -> JsonNull
            is JsonElement -> value
            is String -> JsonPrimitive(value)
            is Number -> JsonPrimitive(value)
            is Boolean -> JsonPrimitive(value)
            is Char -> JsonPrimitive(value.toString())
            is Map<*, *> -> JsonObject(value.entries.associate { (k, v) -> k.toString() to anyToJsonElement(v) })
            is Iterable<*> -> JsonArray(value.map { anyToJsonElement(it) })
            is Array<*> -> JsonArray(value.map { anyToJsonElement(it) })
            else -> {
                try {
                    @Suppress("UNCHECKED_CAST")
                    val serializer = json.serializersModule.serializer(value::class.java) as KSerializer<Any>
                    json.encodeToJsonElement(serializer, value)
                } catch (e: Exception) {
                    JsonPrimitive(value.toString())
                }
            }
        }
    }

    /**
     * Classic ASP.NET SignalR (the /signalr endpoint) frames a hub call as a bare JSON object.
     * The 0x1e record separator belongs to ASP.NET Core SignalR only - appending it here makes
     * the classic server reject the payload.
     */
    fun isClassicEnvelope(text: String): Boolean = asClassicEnvelope(text) != null

    /**
     * Recognises a ready-made classic envelope supplied by the caller, whether it arrives as a raw
     * JSON string or as an already-parsed [JsonObject]. Returns null for anything else.
     */
    fun asClassicEnvelope(value: Any?): JsonObject? {
        val element: JsonElement? = when (value) {
            is JsonObject -> value
            is String -> {
                val trimmed = value.trim().removeSuffix(RECORD_SEPARATOR.toString())
                if (trimmed.startsWith("{")) {
                    try {
                        json.parseToJsonElement(trimmed)
                    } catch (_: Throwable) {
                        null
                    }
                } else {
                    null
                }
            }
            else -> null
        }
        val obj = element as? JsonObject ?: return null
        return if (obj.containsKey("H") || obj.containsKey("M")) obj else null
    }

    /** Serialises an envelope as plain JSON, stamping the id the client is waiting on. */
    fun renderClassicEnvelope(envelope: JsonObject, invocationId: String?): String {
        val fields = if (invocationId == null) {
            envelope
        } else {
            JsonObject(envelope + ("I" to JsonPrimitive(invocationId)))
        }
        return json.encodeToString(JsonObject.serializer(), fields)
    }

    fun createClassicInvocationMessage(
        hub: String,
        method: String,
        invocationId: String?,
        args: Array<out Any>
    ): String {
        val fields = linkedMapOf<String, JsonElement>(
            "H" to JsonPrimitive(hub),
            "M" to JsonPrimitive(method),
            "A" to JsonArray(args.map { anyToJsonElement(it) })
        )
        if (invocationId != null) fields["I"] = JsonPrimitive(invocationId)
        return json.encodeToString(JsonObject.serializer(), JsonObject(fields))
    }

    /**
     * Strips the record separator a caller may have pasted in and stamps the envelope with the
     * invocation id the client is waiting on, so the server's `{"I":..,"R":..}` reply can be matched.
     */
    fun withClassicInvocationId(envelope: String, invocationId: String?): String {
        val obj = asClassicEnvelope(envelope)
            ?: return envelope.trim().removeSuffix(RECORD_SEPARATOR.toString())
        return renderClassicEnvelope(obj, invocationId)
    }

    fun createInvocationMessage(method: String, invocationId: String?, args: Array<out Any>): String {
        val jsonArgs = args.map { anyToJsonElement(it) }
        val msg = HubMessageJson(
            type = 1,
            invocationId = invocationId,
            target = method,
            arguments = jsonArgs
        )
        return json.encodeToString(HubMessageJson.serializer(), msg) + RECORD_SEPARATOR
    }
}
