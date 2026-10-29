package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URLEncoder

sealed class LoginResult {
    data class Success(val sid: String, val rawResponse: String) : LoginResult()
    data class Error(val errorMessage: String) : LoginResult()
}

/** Addresses of the local mock server (`./gradlew :mockserver:run`), reached via `adb reverse`. */
object DemoServer {
    const val HOST = "http://127.0.0.1:8080"
    const val AUTH_URL = "$HOST/api/login"
    const val SIGNALR_URL = "$HOST/signalr/negotiate?connectionData=[{\"name\":\"chathub\"}]&sessionId=demo-placeholder"
}

class DemoAuthService(
    private val baseUrl: String = DemoServer.AUTH_URL,
    private val client: HttpClient = HttpClient(OkHttp),
    private val appId: String = "demo-app"
) {

    suspend fun login(userName: String, pass: String): LoginResult {
        return try {
            val timestamp = System.currentTimeMillis() / 1000
            val rqJson = """[{"action":"Login","args":{"password":"$pass","applicationId":"$appId","userName":"$userName","generateToken":true}}]"""
            val encodedRq = withContext(Dispatchers.IO) {
                URLEncoder.encode(rqJson, "UTF-8")
            }

            val url = "$baseUrl?appid=$appId&timestamp=$timestamp&rq=$encodedRq"

            val response = client.get(url)
            val bodyText = response.bodyAsText()

            val sid = extractSidFromResponse(bodyText)
            if (sid != null) {
                LoginResult.Success(sid, bodyText)
            } else {
                LoginResult.Error("Login response received, but could not extract session ID (sid). Response: $bodyText")
            }
        } catch (t: Throwable) {
            LoginResult.Error("Network/Login error: ${t.localizedMessage ?: t.message}")
        }
    }

    fun extractSidFromResponse(responseText: String): String? {
        try {
            val json = Json { ignoreUnknownKeys = true; isLenient = true }
            val element = json.parseToJsonElement(responseText)
            if (element is JsonArray && element.isNotEmpty()) {
                val firstObj = element[0].jsonObject

                firstObj["sid"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                firstObj["sessionId"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                firstObj["sessionToken"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                firstObj["token"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }

                val resultObj = firstObj["result"]?.jsonObject ?: firstObj["Result"]?.jsonObject ?: firstObj["args"]?.jsonObject
                if (resultObj != null) {
                    resultObj["sid"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                    resultObj["sessionId"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                    resultObj["sessionToken"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                    resultObj["token"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                    resultObj["Token"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                }
            } else if (element is JsonObject) {
                element["sid"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
                element["sessionId"]?.jsonPrimitive?.content?.let { if (it.isNotBlank()) return it }
            }
        } catch (_: Exception) {
            // Fall back to regex parsing
        }

        val patterns = listOf(
            """"sid"\s*:\s*"([^"]+)"""".toRegex(RegexOption.IGNORE_CASE),
            """"sessionId"\s*:\s*"([^"]+)"""".toRegex(RegexOption.IGNORE_CASE),
            """"sessionToken"\s*:\s*"([^"]+)"""".toRegex(RegexOption.IGNORE_CASE),
            """"token"\s*:\s*"([^"]+)"""".toRegex(RegexOption.IGNORE_CASE)
        )

        for (pattern in patterns) {
            val match = pattern.find(responseText)
            if (match != null && match.groupValues.size > 1) {
                val value = match.groupValues[1]
                if (value.isNotBlank() && value != "null") {
                    return value
                }
            }
        }

        return null
    }

    companion object {
        fun updateSessionIdInUrl(url: String, newSid: String): String {
            return if (url.contains("sessionId=")) {
                url.replace("sessionId=[^&]+".toRegex(), "sessionId=$newSid")
            } else if (url.contains("?")) {
                "$url&sessionId=$newSid"
            } else {
                "$url?sessionId=$newSid"
            }
        }
    }
}
