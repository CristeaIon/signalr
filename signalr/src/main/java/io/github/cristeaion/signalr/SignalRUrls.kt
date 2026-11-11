package io.github.cristeaion.signalr

import java.net.URLDecoder
import java.net.URLEncoder

internal fun normalizeAndEncodeQuery(query: String?): String? {
    if (query.isNullOrEmpty()) return query
    return query.split("&").joinToString("&") { pair ->
        val parts = pair.split("=", limit = 2)
        if (parts.size == 2) {
            val key = parts[0]
            val value = parts[1]
            val decodedValue = try {
                URLDecoder.decode(value, "UTF-8")
            } catch (_: Exception) {
                value
            }
            val encodedValue = URLEncoder.encode(decodedValue, "UTF-8")
            "$key=$encodedValue"
        } else {
            pair
        }
    }
}

/**
 * Classic ASP.NET SignalR (2.x) lives under /signalr and carries a `connectionData` query
 * parameter listing the hubs. ASP.NET Core SignalR has neither.
 */
internal fun isClassicSignalRUrl(baseUrl: String): Boolean {
    val parts = baseUrl.split("?", limit = 2)
    return parts[0].contains("/signalr", ignoreCase = true) ||
        (parts.getOrNull(1)?.contains("connectionData", ignoreCase = true) == true)
}

/** Hub names declared in the `connectionData` query parameter, in declaration order. */
internal fun extractClassicHubNames(baseUrl: String): List<String> {
    val rawQuery = baseUrl.split("?", limit = 2).getOrNull(1) ?: return emptyList()
    val connectionData = rawQuery.split("&")
        .firstOrNull { it.startsWith("connectionData=", ignoreCase = true) }
        ?.substringAfter("=")
        ?: return emptyList()
    val decoded = try {
        URLDecoder.decode(connectionData, "UTF-8")
    } catch (_: Exception) {
        connectionData
    }
    return Regex("\"name\"\\s*:\\s*\"([^\"]+)\"", RegexOption.IGNORE_CASE)
        .findAll(decoded)
        .map { it.groupValues[1] }
        .toList()
}

/**
 * Classic SignalR's third handshake step. After /negotiate and the /connect socket the client
 * must call /start over HTTP; only then does the server run OnConnected on each hub and begin
 * routing hub traffic to this connection.
 */
internal fun getStartUrl(baseUrl: String, connectionToken: String?): String =
    classicCommandUrl(baseUrl, connectionToken, "start")

/**
 * Classic SignalR's way to end a connection at once. Without it the server keeps a closed
 * WebSocket's connection alive for its DisconnectTimeout (30s by default), waiting for the
 * client to come back, and runs OnDisconnected only after that.
 */
internal fun getAbortUrl(baseUrl: String, connectionToken: String?): String =
    classicCommandUrl(baseUrl, connectionToken, "abort")

private fun classicCommandUrl(baseUrl: String, connectionToken: String?, command: String): String {
    val parts = baseUrl.split("?", limit = 2)
    val query = normalizeAndEncodeQuery(parts.getOrNull(1))

    val mainUrl = parts[0]
        .replace("ws://", "http://", ignoreCase = true)
        .replace("wss://", "https://", ignoreCase = true)
        .removeSuffix("/")
        .removeSuffix("/negotiate")
        .removeSuffix("/connect")
        .removeSuffix("/")

    val params = mutableListOf<String>()
    if (!query.isNullOrEmpty()) params.add(query)
    if (!connectionToken.isNullOrEmpty() && query?.contains("connectionToken=") != true) {
        params.add("connectionToken=${URLEncoder.encode(connectionToken, "UTF-8")}")
    }
    if (query?.contains("transport=") != true) params.add("transport=webSockets")
    if (query?.contains("clientProtocol=") != true) params.add("clientProtocol=1.5")

    return "$mainUrl/$command?${params.joinToString("&")}"
}

internal fun getNegotiateUrl(baseUrl: String): String {
    val parts = baseUrl.split("?", limit = 2)
    val mainUrl = parts[0]
    val rawQuery = parts.getOrNull(1)
    val query = normalizeAndEncodeQuery(rawQuery)

    val httpMainUrl = mainUrl
        .replace("ws://", "http://", ignoreCase = true)
        .replace("wss://", "https://", ignoreCase = true)

    val negotiateMainUrl = if (httpMainUrl.endsWith("/negotiate") || httpMainUrl.endsWith("/negotiate/")) {
        httpMainUrl.removeSuffix("/")
    } else {
        if (httpMainUrl.endsWith("/")) "${httpMainUrl}negotiate" else "$httpMainUrl/negotiate"
    }

    return if (query.isNullOrEmpty()) negotiateMainUrl else "$negotiateMainUrl?$query"
}

internal fun buildWebSocketUrl(
    baseUrl: String,
    negotiateResponse: NegotiateResponse?,
    token: String?,
): String {
    val parts = baseUrl.split("?", limit = 2)
    val mainUrl = parts[0]
    val rawQuery = parts.getOrNull(1)
    val query = normalizeAndEncodeQuery(rawQuery)

    var wsMainUrl = mainUrl
        .replace("http://", "ws://", ignoreCase = true)
        .replace("https://", "wss://", ignoreCase = true)

    if (!wsMainUrl.startsWith("ws://", ignoreCase = true) && !wsMainUrl.startsWith("wss://", ignoreCase = true)) {
        wsMainUrl = "wss://$wsMainUrl"
    }

    val isClassicSignalR = isClassicSignalRUrl(baseUrl)

    if (wsMainUrl.endsWith("/negotiate") || wsMainUrl.endsWith("/negotiate/")) {
        val cleanMainUrl = wsMainUrl.removeSuffix("/").removeSuffix("/negotiate")
        wsMainUrl = if (isClassicSignalR) {
            "$cleanMainUrl/connect"
        } else {
            cleanMainUrl
        }
    } else if (isClassicSignalR && !wsMainUrl.endsWith("/connect") && !wsMainUrl.endsWith("/connect/")) {
        wsMainUrl = if (wsMainUrl.endsWith("/")) {
            "${wsMainUrl}connect"
        } else {
            "$wsMainUrl/connect"
        }
    }

    val params = mutableListOf<String>()
    if (!query.isNullOrEmpty()) {
        params.add(query)
    }

    val connectionToken = negotiateResponse?.getEffectiveConnectionToken() ?: negotiateResponse?.getEffectiveConnectionId()

    if (!connectionToken.isNullOrEmpty()) {
        val encodedToken = URLEncoder.encode(connectionToken, "UTF-8")
        if (isClassicSignalR) {
            if (query == null || !query.contains("connectionToken=")) {
                params.add("connectionToken=$encodedToken")
            }
        } else {
            if (query == null || !query.contains("id=")) {
                params.add("id=$encodedToken")
            }
        }
    }

    if (isClassicSignalR) {
        if (query == null || !query.contains("transport=")) {
            params.add("transport=webSockets")
        }
    }

    if (!token.isNullOrEmpty() && (query == null || !query.contains("access_token="))) {
        val encodedAuth = URLEncoder.encode(token, "UTF-8")
        params.add("access_token=$encodedAuth")
    }

    return if (params.isNotEmpty()) {
        "$wsMainUrl?${params.joinToString("&")}"
    } else {
        wsMainUrl
    }
}

/**
 * [url] for logs below VERBOSE: the query is dropped because it can carry credentials
 * (`sessionId`, `access_token`, `connectionToken`). A trailing `?…` shows that one was there.
 */
internal fun withoutQuery(url: String): String {
    val end = url.indexOfAny(charArrayOf('?', '#'))
    return if (end < 0) url else url.substring(0, end) + "?…"
}
