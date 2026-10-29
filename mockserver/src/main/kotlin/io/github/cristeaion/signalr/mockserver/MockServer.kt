package io.github.cristeaion.signalr.mockserver

import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.cio.CIOApplicationEngine
import io.ktor.server.engine.EmbeddedServer
import io.ktor.server.engine.embeddedServer
import io.ktor.server.response.respondText
import io.ktor.server.routing.RoutingContext
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.CloseReason
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/** Demo accounts accepted by the mock auth. The first one matches the app's pre-filled login. */
val DEMO_USERS = linkedMapOf(
    "sa@example.com" to "111111",
    "alice@example.com" to "111111",
    "bob@example.com" to "111111",
)

/**
 * Local test fixture: a mock login endpoint plus a classic SignalR 2.x chat hub.
 *
 * Login:   GET  /api/login?appid=&timestamp=&rq=[{"action":"Login","args":{"userName":..,"password":..}}]
 * SignalR: /signalr/negotiate, /signalr/connect (WebSocket), /signalr/start, /signalr/abort, /signalr/ping
 * Faults:  POST /mock/control/{reject-next-negotiate|fail-next-invoke|drop-all|init-delay?ms=N|reset}
 */
class MockServer(
    private val host: String = "127.0.0.1",
    users: Map<String, String> = DEMO_USERS,
) {
    val hub = ChatHub(users)
    private var server: EmbeddedServer<CIOApplicationEngine, CIOApplicationEngine.Configuration>? = null

    /** Starts the server and returns the bound port; pass 0 to pick a free one. */
    fun start(port: Int = 0): Int {
        val embedded = embeddedServer(CIO, port = port, host = host) { module(hub) }
        embedded.start(wait = false)
        server = embedded
        return runBlocking { embedded.engine.resolvedConnectors().first().port }
    }

    fun stop() {
        server?.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
        server = null
    }
}

internal fun Application.module(hub: ChatHub) {
    install(WebSockets)
    routing {
        get("/api/login") { handleLogin(hub) }

        route("/signalr") {
            get("/negotiate") { handleNegotiate(hub) }
            post("/negotiate") { handleNegotiate(hub) }
            get("/start") {
                val connection = hub.connection(call.request.queryParameters["connectionToken"])
                if (connection == null) {
                    respondJson("""{"Error":"Unknown connection token"}""", HttpStatusCode.BadRequest)
                    return@get
                }
                hub.start(connection)
                respondJson("""{"Response":"started"}""")
            }
            get("/abort") { handleAbort(hub) }
            post("/abort") { handleAbort(hub) }
            get("/ping") { respondJson("""{"Response":"pong"}""") }

            webSocket("/connect") {
                val connection = hub.connection(call.request.queryParameters["connectionToken"])
                if (connection == null) {
                    close(CloseReason(CloseReason.Codes.VIOLATED_POLICY, "Unknown connection token"))
                    return@webSocket
                }
                connection.socket = this
                try {
                    delay(hub.faults.initFrameDelayMs)
                    send(Frame.Text(hub.initFrame()))
                    for (frame in incoming) {
                        if (frame !is Frame.Text) continue
                        hub.handleClientFrame(connection, frame.readText())?.let { send(Frame.Text(it)) }
                    }
                } finally {
                    hub.close(connection)
                }
            }
        }

        post("/mock/control/{action}") {
            val faults = hub.faults
            when (call.parameters["action"]) {
                "reject-next-negotiate" -> faults.rejectNextNegotiate.set(true)
                "fail-next-invoke" -> faults.failNextInvoke.set(true)
                "drop-all" -> hub.dropAll()
                "init-delay" -> faults.initFrameDelayMs = call.request.queryParameters["ms"]?.toLongOrNull() ?: 0
                "reset" -> faults.reset()
                else -> {
                    respondJson("""{"Error":"Unknown action"}""", HttpStatusCode.NotFound)
                    return@post
                }
            }
            call.respondText("", status = HttpStatusCode.NoContent)
        }
    }
}

private suspend fun RoutingContext.handleLogin(hub: ChatHub) {
    val args = runCatching {
        val rq = call.request.queryParameters["rq"]!!
        (Json.parseToJsonElement(rq) as JsonArray)[0].jsonObject["args"]!!.jsonObject
    }.getOrNull()
    if (args == null) {
        respondJson("""[{"action":"Login","error":"Malformed request"}]""", HttpStatusCode.BadRequest)
        return
    }
    val sid = hub.login(
        args["userName"]?.jsonPrimitive?.contentOrNull.orEmpty(),
        args["password"]?.jsonPrimitive?.contentOrNull.orEmpty(),
    )
    if (sid == null) {
        respondJson("""[{"action":"Login","error":"Invalid credentials"}]""")
    } else {
        respondJson("""[{"action":"Login","sid":"$sid"}]""")
    }
}

private suspend fun RoutingContext.handleNegotiate(hub: ChatHub) {
    if (hub.faults.rejectNextNegotiate.compareAndSet(true, false)) {
        respondJson("""{"Error":"Mock negotiate failure"}""", HttpStatusCode.InternalServerError)
        return
    }
    val user = hub.userForSession(call.request.queryParameters["sessionId"])
    if (user == null) {
        respondJson("""{"Error":"Unknown session"}""", HttpStatusCode.Unauthorized)
        return
    }
    val connection = hub.openConnection(user)
    respondJson(
        """{"Url":"/signalr","ConnectionToken":"${connection.token}","ConnectionId":"${connection.token}",""" +
            """"KeepAliveTimeout":20.0,"DisconnectTimeout":30.0,"TryWebSockets":true,""" +
            """"ProtocolVersion":"1.5","TransportConnectTimeout":5.0}"""
    )
}

private suspend fun RoutingContext.handleAbort(hub: ChatHub) {
    hub.connection(call.request.queryParameters["connectionToken"])?.let { connection ->
        runCatching { connection.socket?.close(CloseReason(CloseReason.Codes.NORMAL, "Aborted")) }
        hub.close(connection)
    }
    respondJson("{}")
}

private suspend fun RoutingContext.respondJson(body: String, status: HttpStatusCode = HttpStatusCode.OK) {
    call.respondText(body, ContentType.Application.Json, status)
}
