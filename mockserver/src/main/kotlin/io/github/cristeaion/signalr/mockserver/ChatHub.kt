package io.github.cristeaion.signalr.mockserver

import io.ktor.websocket.CloseReason
import io.ktor.websocket.DefaultWebSocketSession
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

const val CHAT_HUB = "chathub"
const val BOT_NAME = "ChatBot"

data class ChatMessage(val user: String, val message: String, val timestamp: Long)

/** One switch per failure mode; each one-shot flag resets after it fires (US6). */
class FaultSwitches {
    val rejectNextNegotiate = AtomicBoolean(false)
    val failNextInvoke = AtomicBoolean(false)
    @Volatile var initFrameDelayMs: Long = 0

    fun reset() {
        rejectNextNegotiate.set(false)
        failNextInvoke.set(false)
        initFrameDelayMs = 0
    }
}

internal class Connection(val token: String, val user: String) {
    @Volatile var socket: DefaultWebSocketSession? = null
    @Volatile var started = false
}

/**
 * In-memory state of the mock: issued sessions, live connections and the single chat room.
 * Speaks classic SignalR 2.x framing - bare JSON objects, no 0x1e separators.
 */
class ChatHub(private val users: Map<String, String>) {

    private val sessions = ConcurrentHashMap<String, String>()
    private val connections = ConcurrentHashMap<String, Connection>()
    private val history = CopyOnWriteArrayList<ChatMessage>()
    private val sessionCounter = AtomicLong(1)
    private val messageCursor = AtomicLong(0)
    val faults = FaultSwitches()

    /** Returns a new session id, or null when the credentials don't match a demo user. */
    fun login(userName: String, password: String): String? {
        if (users[userName] != password) return null
        val sid = "demo~@~${sessionCounter.getAndIncrement()}_${UUID.randomUUID()}"
        sessions[sid] = userName
        return sid
    }

    fun userForSession(sid: String?): String? = sid?.let { sessions[it] }

    internal fun openConnection(user: String): Connection {
        val connection = Connection(UUID.randomUUID().toString(), user)
        connections[connection.token] = connection
        return connection
    }

    internal fun connection(token: String?): Connection? = token?.let { connections[it] }

    fun onlineUsers(): List<String> =
        connections.values.filter { it.started }.map { it.user }.distinct().sorted()

    fun history(): List<ChatMessage> = history.toList()

    internal suspend fun start(connection: Connection) {
        connection.started = true
        broadcast("UserJoined", listOf(JsonPrimitive(connection.user)), except = connection)
    }

    internal suspend fun close(connection: Connection) {
        if (connections.remove(connection.token) == null) return
        if (connection.started) {
            broadcast("UserLeft", listOf(JsonPrimitive(connection.user)), except = connection)
        }
    }

    /** Pushes a chat message to everyone in the room without a bot reply. */
    suspend fun postMessage(user: String, message: String) {
        val entry = ChatMessage(user, message, System.currentTimeMillis())
        history.add(entry)
        while (history.size > HISTORY_LIMIT) history.removeAt(0)
        broadcast("ReceiveMessage", listOf(JsonPrimitive(user), JsonPrimitive(message)))
    }

    /** Closes every socket, as if the server went down. */
    suspend fun dropAll() {
        connections.values.toList().forEach { connection ->
            runCatching { connection.socket?.close(CloseReason(CloseReason.Codes.GOING_AWAY, "Mock server drop")) }
            close(connection)
        }
    }

    fun initFrame(): String = """{"C":"${cursor()}","S":1,"M":[]}"""

    /** Handles one client frame and returns the reply to send back, if any. */
    internal suspend fun handleClientFrame(connection: Connection, text: String): String? {
        val frame = runCatching { Json.parseToJsonElement(text).jsonObject }.getOrNull() ?: return null
        val hub = frame["H"]?.jsonPrimitive?.contentOrNull
        val method = frame["M"]?.jsonPrimitive?.contentOrNull ?: return null
        val args = (frame["A"] as? JsonArray) ?: JsonArray(emptyList())
        val id = frame["I"]?.jsonPrimitive?.contentOrNull

        if (id != null && faults.failNextInvoke.compareAndSet(true, false)) {
            return completion(id, error = "Mock failure")
        }
        if (!hub.equals(CHAT_HUB, ignoreCase = true)) {
            return id?.let { completion(it, error = "Hub '$hub' not found") }
        }

        return when (method.lowercase()) {
            "send" -> {
                val message = args.getOrNull(0)?.jsonPrimitive?.contentOrNull.orEmpty()
                postMessage(connection.user, message)
                postMessage(BOT_NAME, botReply(connection.user, message))
                id?.let { completion(it) }
            }
            "gethistory" -> id?.let {
                completion(it, result = buildJsonArray {
                    history.forEach { entry ->
                        add(buildJsonObject {
                            put("user", entry.user)
                            put("message", entry.message)
                            put("timestamp", entry.timestamp)
                        })
                    }
                })
            }
            "getonlineusers" -> id?.let {
                completion(it, result = JsonArray(onlineUsers().map { user -> JsonPrimitive(user) }))
            }
            else -> id?.let { completion(it, error = "Method '$hub.$method' not found") }
        }
    }

    private fun botReply(user: String, message: String): String =
        "Hi ${user.substringBefore('@')}, you said: \"$message\""

    private suspend fun broadcast(event: String, args: List<JsonElement>, except: Connection? = null) {
        val frame = buildJsonObject {
            put("C", cursor())
            put("M", buildJsonArray {
                add(buildJsonObject {
                    put("H", CHAT_HUB)
                    put("M", event)
                    put("A", JsonArray(args))
                })
            })
        }.toString()
        connections.values
            .filter { it.started && it !== except }
            .forEach { connection -> runCatching { connection.socket?.send(Frame.Text(frame)) } }
    }

    private fun completion(id: String, result: JsonElement? = null, error: String? = null): String =
        JsonObject(buildMap {
            put("I", JsonPrimitive(id))
            if (result != null) put("R", result)
            if (error != null) put("E", JsonPrimitive(error))
        }).toString()

    private fun cursor() = "d-${messageCursor.incrementAndGet()}"

    private companion object {
        const val HISTORY_LIMIT = 50
    }
}
