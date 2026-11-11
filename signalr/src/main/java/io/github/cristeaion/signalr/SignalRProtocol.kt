package io.github.cristeaion.signalr

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.serializer

/** Encoding and decoding of SignalR wire frames, for both ASP.NET Core and classic servers. */
internal object SignalRProtocol {
    const val RECORD_SEPARATOR = 0x1e.toChar()
    val json: Json get() = signalRJson

    fun createHandshakeMessage(): String {
        return json.encodeToString(HandshakeRequest.serializer(), HandshakeRequest()) + RECORD_SEPARATOR
    }

    /** ASP.NET Core keep-alive: a type-6 message. Neither side answers it; receiving it is enough. */
    val PING_MESSAGE: String = "{\"type\":6}$RECORD_SEPARATOR"

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
                // Sending value.toString() instead would hand the server data it can't use, with
                // nothing failing on this side.
                val serializer = try {
                    @Suppress("UNCHECKED_CAST")
                    json.serializersModule.serializer(value::class.java) as KSerializer<Any>
                } catch (e: Exception) {
                    throw IllegalArgumentException(
                        "Cannot send an argument of type ${value::class.qualifiedName}: annotate it with " +
                            "@Serializable, or pass a primitive, Map, collection, array or JsonElement.",
                        e,
                    )
                }
                json.encodeToJsonElement(serializer, value)
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
        args: Array<out Any?>
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

    fun createInvocationMessage(method: String, invocationId: String?, args: Array<out Any?>): String {
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
