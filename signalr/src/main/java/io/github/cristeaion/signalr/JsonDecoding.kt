package io.github.cristeaion.signalr

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement

/**
 * Decodes [element] as [T], mapping JSON `null` to a type-appropriate default for non-nullable
 * primitives. Internal, but published because the public inline extensions inline a call to it.
 */
@PublishedApi
internal inline fun <reified T> decodeJsonElementSafe(element: JsonElement): T {
    if (element is JsonNull) {
        if (null is T) {
            return null as T
        }
        val defaultVal: Any? = when (T::class) {
            String::class -> ""
            Long::class -> 0L
            Int::class -> 0
            Double::class -> 0.0
            Float::class -> 0f
            Boolean::class -> false
            JsonElement::class -> JsonNull
            JsonObject::class -> JsonObject(emptyMap<String, JsonElement>())
            JsonArray::class -> JsonArray(emptyList<JsonElement>())
            else -> null
        }
        if (defaultVal != null) {
            return defaultVal as T
        }
    }
    return signalRJson.decodeFromJsonElement(element)
}

/** The JSON configuration shared by every SignalR frame this library reads or writes. */
@PublishedApi
internal val signalRJson: Json = Json {
    ignoreUnknownKeys = true
    encodeDefaults = true
    isLenient = true
}
