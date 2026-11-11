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
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.URLEncoder
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Classic SignalR 2.x: the server's D (disconnect) and T (reconnect) commands, and /abort. */
class ClassicCommandsTest {

    private val connects = AtomicInteger(0)
    private val tokens = AtomicInteger(0)
    private val aborts = CopyOnWriteArrayList<String>()
    @Volatile private var current: DefaultWebSocketServerSession? = null
    @Volatile private var startStatus = HttpStatusCode.OK

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            post("/signalr/negotiate") {
                call.respondText(
                    """{"ConnectionToken":"tok-${tokens.incrementAndGet()}","ConnectionId":"id","ProtocolVersion":"1.5","TryWebSockets":true}""",
                    ContentType.Application.Json,
                )
            }
            get("/signalr/start") {
                call.respondText("""{"Response":"started","Secret":"start-body"}""", ContentType.Application.Json, startStatus)
            }
            post("/signalr/abort") {
                aborts += call.request.uri
                call.respondText("")
            }
            webSocket("/signalr/connect") {
                connects.incrementAndGet()
                current = this
                send(Frame.Text("""{"C":"s-0","S":1,"M":[]}"""))
                for (frame in incoming) Unit
            }
        }
    }.also { it.start(wait = false) }

    private val port = runBlocking { server.engine.resolvedConnectors().first().port }
    private val url = "http://127.0.0.1:$port/signalr?connectionData=" +
        URLEncoder.encode("""[{"name":"chathub"}]""", "UTF-8")

    private val client = HttpClient(OkHttp) { install(ClientWebSockets) }

    private fun connection(policy: RetryPolicy? = null) = HubConnectionImpl(
        client = client,
        baseUrl = url,
        connectionHeaders = emptyMap(),
        accessTokenProvider = null,
        skipNegotiate = false,
        logger = NoOpSignalRLogger,
        handshakeTimeoutMs = 2_000,
        retryPolicy = policy,
    )

    @After
    fun tearDown() {
        client.close()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private suspend fun HubConnection.awaitState(state: HubConnectionState) =
        withTimeout(5_000) { connectionState.first { it == state } }

    private suspend fun awaitAborts(count: Int) = withTimeout(3_000) { while (aborts.size < count) delay(10) }

    @Test
    fun aFailedStartExposesItsStatusButNotItsBody() {
        startStatus = HttpStatusCode.Forbidden

        val error = assertThrows(SignalRHttpException::class.java) { runBlocking { connection().connect() } }

        assertEquals(403, error.statusCode)
        assertTrue(error.isAuthFailure)
        assertFalse(error.message, error.message!!.contains("start-body"))
    }

    @Test
    fun disconnectSendsAbortForTheConnection() = runBlocking {
        val connection = connection()
        connection.connect()

        connection.disconnect()

        awaitAborts(1)
        assertTrue(aborts.single(), aborts.single().contains("connectionToken=tok-1"))
        assertTrue(aborts.single(), aborts.single().contains("connectionData="))
    }

    @Test
    fun closeSendsAbortBeforeReleasingTheClient() = runBlocking {
        val connection = connection()
        connection.connect()

        connection.close()

        awaitAborts(1)
        assertEquals(1, aborts.size)
    }

    @Test
    fun abortIsSentOnlyOnce() = runBlocking {
        val connection = connection()
        connection.connect()
        connection.disconnect()
        connection.disconnect()
        connection.close()

        awaitAborts(1)
        delay(300)
        assertEquals(1, aborts.size)
    }

    @Test
    fun disconnectCommandEndsTheConnectionWithoutReconnecting() = runBlocking {
        val connection = connection(policy = RetryPolicy { 0L })
        val received = CopyOnWriteArrayList<String>()
        connection.on<String>("Notify") { received += it }
        connection.connect()

        // Invocations batched with the command are still delivered.
        current!!.send(Frame.Text("""{"C":"s-1","M":[{"H":"chathub","M":"Notify","A":["bye"]}],"D":1}"""))
        connection.awaitState(HubConnectionState.DISCONNECTED)

        delay(300)
        assertEquals(1, connects.get())
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        withTimeout(2_000) { while (received.isEmpty()) delay(10) }
        assertEquals(listOf("bye"), received)
    }

    @Test
    fun reconnectCommandReconnects() = runBlocking {
        val connection = connection(policy = RetryPolicy { 0L })
        connection.connect()

        current!!.send(Frame.Text("""{"T":1}"""))
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.CONNECTED)

        assertEquals(2, connects.get())
        connection.disconnect()
        awaitAborts(1)
        // Only the connection that was current is aborted, with its own token.
        assertTrue(aborts.single(), aborts.single().contains("connectionToken=tok-2"))
    }

    @Test
    fun aDroppedSocketStillReconnects() = runBlocking {
        val connection = connection(policy = RetryPolicy { 0L })
        connection.connect()

        current!!.close()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.CONNECTED)

        assertEquals(2, connects.get())
        connection.disconnect()
    }
}
