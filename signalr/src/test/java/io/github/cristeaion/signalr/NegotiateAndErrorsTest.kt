package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.request.uri
import io.ktor.server.response.respondText
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * Negotiate redirects and transport checks, typed errors, and null arguments, against a minimal
 * ASP.NET Core-style server. `/hub` behaves as [negotiateMode] says; `/azure` plays the
 * service a redirect points to. Every hub echoes `Echo(x)` and fails `Fail()` with a hub error.
 */
class NegotiateAndErrorsTest {

    private enum class Mode { OK, STATUS_401, STATUS_500, ERROR_FIELD, NO_WEBSOCKETS, BINARY_ONLY, REDIRECT, REDIRECT_LOOP }

    private val rs = SignalRProtocol.RECORD_SEPARATOR
    private val okNegotiate =
        """{"connectionToken":"ct-1","connectionId":"c1","negotiateVersion":1,"availableTransports":[{"transport":"WebSockets","transferFormats":["Text","Binary"]}]}"""

    @Volatile private var negotiateMode = Mode.OK
    private val loopNegotiates = AtomicInteger(0)
    private val azureAuthorization = CopyOnWriteArrayList<String?>()
    private val socketUris = CopyOnWriteArrayList<String>()
    private val received = CopyOnWriteArrayList<String>()
    @Volatile private var current: DefaultWebSocketServerSession? = null

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            post("/hub/negotiate") {
                val (status, body) = when (negotiateMode) {
                    Mode.OK -> HttpStatusCode.OK to okNegotiate
                    Mode.STATUS_401 -> HttpStatusCode.Unauthorized to ""
                    Mode.STATUS_500 -> HttpStatusCode.InternalServerError to "secret-stack-trace"
                    Mode.ERROR_FIELD -> HttpStatusCode.OK to """{"error":"Hub is disabled"}"""
                    Mode.NO_WEBSOCKETS -> HttpStatusCode.OK to
                        """{"connectionToken":"x","negotiateVersion":1,"availableTransports":[{"transport":"ServerSentEvents","transferFormats":["Text"]},{"transport":"LongPolling","transferFormats":["Text","Binary"]}]}"""
                    Mode.BINARY_ONLY -> HttpStatusCode.OK to
                        """{"connectionToken":"x","negotiateVersion":1,"availableTransports":[{"transport":"WebSockets","transferFormats":["Binary"]}]}"""
                    Mode.REDIRECT -> HttpStatusCode.OK to """{"url":"$base/azure/?hub=chat","accessToken":"azure-token"}"""
                    Mode.REDIRECT_LOOP -> {
                        loopNegotiates.incrementAndGet()
                        HttpStatusCode.OK to """{"url":"$base/hub","accessToken":"t"}"""
                    }
                }
                call.respondText(body, ContentType.Application.Json, status)
            }
            post("/azure/negotiate") {
                azureAuthorization += call.request.headers["Authorization"]
                call.respondText(okNegotiate.replace("ct-1", "azure-ct"), ContentType.Application.Json)
            }
            webSocket("/hub") { serve() }
            webSocket("/azure/") { serve() }
        }
    }.also { it.start(wait = false) }

    private suspend fun DefaultWebSocketServerSession.serve() {
        socketUris += call.request.uri
        for (frame in incoming) {
            if (frame !is Frame.Text) continue
            for (text in SignalRProtocol.parseFrames(frame.readText())) {
                if (text.contains("\"protocol\"")) {
                    send(Frame.Text("{}$rs"))
                    current = this
                    continue
                }
                received += text
                val msg = SignalRProtocol.json.decodeFromString<HubMessageJson>(text)
                val id = msg.invocationId ?: continue
                when (msg.target) {
                    "Echo" -> send(Frame.Text("""{"type":3,"invocationId":"$id","result":${msg.arguments!!.first()}}$rs"""))
                    "Fail" -> send(Frame.Text("""{"type":3,"invocationId":"$id","error":"Method not allowed"}$rs"""))
                }
            }
        }
    }

    private val base: String = "http://127.0.0.1:${runBlocking { server.engine.resolvedConnectors().first().port }}"

    private val client = HttpClient(OkHttp) { install(ClientWebSockets) }

    private fun connection(skipNegotiate: Boolean = false) = HubConnectionImpl(
        client = client,
        baseUrl = "$base/hub",
        connectionHeaders = emptyMap(),
        accessTokenProvider = { "app-token" },
        skipNegotiate = skipNegotiate,
        logger = NoOpSignalRLogger,
        handshakeTimeoutMs = 2_000,
        keepAliveIntervalMs = 0,
        serverTimeoutMs = 0,
    )

    @After
    fun tearDown() {
        client.close()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private inline fun <reified T : Throwable> connectFails(): T =
        assertThrows(T::class.java) { runBlocking { connection().connect() } }

    // --- 11: negotiate redirect and transports ---

    @Test
    fun negotiateRedirectIsFollowedWithTheServiceToken() = runBlocking {
        negotiateMode = Mode.REDIRECT
        val connection = connection()

        connection.connect()

        assertEquals(listOf("Bearer azure-token"), azureAuthorization)
        val uri = socketUris.single()
        assertTrue(uri, uri.startsWith("/azure/?"))
        assertTrue(uri, uri.contains("hub=chat"))
        assertTrue(uri, uri.contains("id=azure-ct"))
        assertTrue(uri, uri.contains("access_token=azure-token"))
        assertFalse(uri, uri.contains("app-token"))
        assertEquals(JsonPrimitive("hi"), connection.invoke("Echo", "hi"))
        connection.disconnect()
    }

    @Test
    fun endlessRedirectsFail() {
        negotiateMode = Mode.REDIRECT_LOOP

        val error = connectFails<NegotiateException>()

        assertTrue(error.message, error.message!!.contains("100"))
        assertEquals(101, loopNegotiates.get())
        assertTrue(socketUris.isEmpty())
    }

    @Test
    fun aServerWithoutWebSocketsFailsClearly() {
        negotiateMode = Mode.NO_WEBSOCKETS

        val error = connectFails<NegotiateException>()

        assertTrue(error.message, error.message!!.contains("WebSockets"))
        assertTrue(error.message, error.message!!.contains("LongPolling"))
        assertTrue(socketUris.isEmpty())
    }

    @Test
    fun webSocketsWithoutTextFramesFailsClearly() {
        negotiateMode = Mode.BINARY_ONLY

        val error = connectFails<NegotiateException>()

        assertTrue(error.message, error.message!!.contains("text"))
    }

    // --- 6: typed errors ---

    @Test
    fun negotiateStatusIsExposed() {
        negotiateMode = Mode.STATUS_401
        val unauthorized = connectFails<NegotiateException>()
        assertEquals(401, unauthorized.statusCode)
        assertTrue(unauthorized.isAuthFailure)

        negotiateMode = Mode.STATUS_500
        val outage = connectFails<SignalRHttpException>()
        assertEquals(500, outage.statusCode)
        assertFalse(outage.isAuthFailure)
        assertFalse(outage.message, outage.message!!.contains("secret-stack-trace"))
    }

    @Test
    fun negotiateErrorFieldHasNoStatus() {
        negotiateMode = Mode.ERROR_FIELD

        val error = connectFails<NegotiateException>()

        assertNull(error.statusCode)
        assertTrue(error.message, error.message!!.contains("Hub is disabled"))
    }

    @Test
    fun aFailedHubMethodThrowsHubException() = runBlocking {
        val connection = connection()
        connection.connect()

        val error = assertThrows(HubException::class.java) { runBlocking { connection.invoke("Fail") } }

        assertEquals("Method not allowed", error.serverError)
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun aCloseMessageWithAnErrorEmitsHubException() = runBlocking {
        val connection = connection()
        connection.connect()
        val error = async(start = CoroutineStart.UNDISPATCHED) { connection.errorFlow.filterIsInstance<HubException>().first() }

        current!!.send(Frame.Text("""{"type":7,"error":"Kicked by admin"}$rs"""))

        assertEquals("Kicked by admin", withTimeout(3_000) { error.await() }.serverError)
    }

    // --- 7: null arguments ---

    @Test
    fun nullArgumentsAreSentAsJsonNull() = runBlocking {
        val connection = connection()
        connection.connect()

        connection.send("Store", null, "x", null)
        assertEquals(JsonNull, connection.invoke("Echo", null))
        val typed: String? = connection.invoke<String?>("Echo", null, timeoutMs = 2_000)

        assertNull(typed)
        val store = received.first { it.contains("\"Store\"") }
        assertTrue(store, store.contains("\"arguments\":[null,\"x\",null]"))
        connection.disconnect()
    }
}
