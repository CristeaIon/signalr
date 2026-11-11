package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.filterIsInstance
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** Automatic reconnect, driven over real sockets against a minimal ASP.NET Core-style hub. */
class HubConnectionReconnectTest {

    private val rs = SignalRProtocol.RECORD_SEPARATOR

    /** Handshakes the server refuses before it accepts one again. */
    private val handshakesToReject = AtomicInteger(0)
    /** Every WebSocket the server accepted, including refused handshakes. */
    private val socketsOpened = AtomicInteger(0)
    /** Handshakes the server accepted. */
    private val connections = AtomicInteger(0)
    @Volatile private var current: DefaultWebSocketServerSession? = null

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            webSocket("/hub") {
                socketsOpened.incrementAndGet()
                for (frame in incoming) {
                    if (frame !is Frame.Text) continue
                    if (!frame.readText().contains("\"protocol\"")) continue
                    if (handshakesToReject.getAndUpdate { if (it > 0) it - 1 else 0 } > 0) {
                        send(Frame.Text("{\"error\":\"Server is busy\"}$rs"))
                        close()
                        return@webSocket
                    }
                    send(Frame.Text("{}$rs"))
                    connections.incrementAndGet()
                    current = this
                }
            }
        }
    }.also { it.start(wait = false) }

    private val url = "http://127.0.0.1:${runBlocking { server.engine.resolvedConnectors().first().port }}/hub"

    private val client = HttpClient(OkHttp) { install(ClientWebSockets) }

    private fun connection(policy: RetryPolicy?, serverTimeoutMs: Long = 0) = HubConnectionImpl(
        client = client,
        baseUrl = url,
        connectionHeaders = emptyMap(),
        accessTokenProvider = null,
        skipNegotiate = true,
        logger = NoOpSignalRLogger,
        handshakeTimeoutMs = 2_000,
        keepAliveIntervalMs = 0,
        serverTimeoutMs = serverTimeoutMs,
        retryPolicy = policy,
    )

    @After
    fun tearDown() {
        client.close()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private suspend fun dropFromServer() = current!!.close()

    private suspend fun HubConnection.awaitState(state: HubConnectionState) =
        withTimeout(5_000) { connectionState.first { it == state } }

    @Test
    fun droppedConnectionReconnectsAndKeepsHandlers() = runBlocking {
        val connection = connection(RetryPolicy.fixedDelays(listOf(0, 50)))
        val received = CopyOnWriteArrayList<String>()
        connection.on<String>("Notify") { received += it }
        connection.connect()
        val reconnected = async { connection.reconnectedFlow.first() }

        dropFromServer()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.CONNECTED)
        withTimeout(2_000) { reconnected.await() }
        assertEquals(2, connections.get())

        current!!.send(Frame.Text("{\"type\":1,\"target\":\"Notify\",\"arguments\":[\"after\"]}$rs"))
        withTimeout(2_000) { while (received.isEmpty()) delay(10) }
        assertEquals(listOf("after"), received)
        connection.send("Echo", "works") // the new session is usable
        connection.disconnect()
    }

    @Test
    fun failedAttemptsAreRetriedWithGrowingCount() = runBlocking {
        val contexts = CopyOnWriteArrayList<RetryContext>()
        val connection = connection(RetryPolicy { context -> contexts += context; 20L })
        connection.connect()

        handshakesToReject.set(2)
        dropFromServer()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.CONNECTED)

        assertEquals(listOf(0, 1, 2), contexts.map { it.previousRetryCount })
        assertTrue(
            "the failed handshake should be the retry reason, got ${contexts[1].retryReason}",
            contexts[1].retryReason?.message?.contains("Server is busy") == true,
        )
        assertEquals(2, connections.get())
        connection.disconnect()
    }

    @Test
    fun givesUpWhenThePolicyStopsAndReportsIt() = runBlocking {
        val connection = connection(RetryPolicy.fixedDelays(listOf(10, 10)))
        connection.connect()
        val failure = async { connection.errorFlow.filterIsInstance<ReconnectFailedException>().first() }

        handshakesToReject.set(Int.MAX_VALUE)
        dropFromServer()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.DISCONNECTED)

        val error = withTimeout(2_000) { failure.await() }
        assertEquals(2, error.attempts)
        assertTrue(error.cause?.message?.contains("Server is busy") == true)
        // Giving up leaves the connection reusable.
        handshakesToReject.set(0)
        connection.connect()
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun disconnectStopsAPendingReconnect() = runBlocking {
        val connection = connection(RetryPolicy { 300L })
        connection.connect()

        dropFromServer()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.disconnect()

        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        delay(600)
        assertEquals(1, socketsOpened.get())
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun closeStopsAPendingReconnect() = runBlocking {
        val connection = connection(RetryPolicy { 300L })
        connection.connect()

        dropFromServer()
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.close()

        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        delay(600)
        assertEquals(1, socketsOpened.get())
    }

    @Test
    fun serverCloseWithoutAllowReconnectDisconnects() = runBlocking {
        val connection = connection(RetryPolicy { 0L })
        connection.connect()
        val error = async { connection.errorFlow.first() }

        current!!.send(Frame.Text("{\"type\":7,\"error\":\"Unauthorized\"}$rs"))
        connection.awaitState(HubConnectionState.DISCONNECTED)

        assertTrue(withTimeout(2_000) { error.await() }.message!!.contains("Unauthorized"))
        delay(200)
        assertEquals(1, socketsOpened.get())
    }

    @Test
    fun serverCloseWithAllowReconnectReconnects() = runBlocking {
        val connection = connection(RetryPolicy { 0L })
        connection.connect()

        current!!.send(Frame.Text("{\"type\":7,\"allowReconnect\":true}$rs"))
        connection.awaitState(HubConnectionState.RECONNECTING)
        connection.awaitState(HubConnectionState.CONNECTED)

        assertEquals(2, connections.get())
        connection.disconnect()
    }

    @Test
    fun serverTimeoutTriggersReconnect() = runBlocking {
        val connection = connection(RetryPolicy.fixedDelays(listOf(0)), serverTimeoutMs = 300)
        val timeout = async { connection.errorFlow.filterIsInstance<ServerTimeoutException>().first() }
        connection.connect()

        withTimeout(3_000) { timeout.await() }
        connection.awaitState(HubConnectionState.CONNECTED)
        assertTrue(connections.get() >= 2)
        connection.disconnect()
    }

    @Test
    fun withoutAPolicyADropStaysDisconnected() = runBlocking {
        val connection = connection(policy = null)
        connection.connect()

        dropFromServer()
        connection.awaitState(HubConnectionState.DISCONNECTED)
        delay(200)

        assertEquals(1, socketsOpened.get())
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun aFailedFirstConnectIsNotRetried() = runBlocking {
        handshakesToReject.set(1)
        val connection = connection(RetryPolicy { 0L })

        runCatching { connection.connect() }
        delay(200)

        assertEquals(1, socketsOpened.get())
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }
}
