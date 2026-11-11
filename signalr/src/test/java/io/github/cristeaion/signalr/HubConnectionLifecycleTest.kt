package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.async
import kotlinx.coroutines.job
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Drives the real client over real sockets against a minimal in-process ASP.NET Core-style hub.
 * Each test picks how the server behaves after the WebSocket opens.
 */
class HubConnectionLifecycleTest {

    private enum class Handshake { OK, ERROR, NONE }

    private val rs = SignalRProtocol.RECORD_SEPARATOR

    @Volatile private var handshake = Handshake.OK
    /** When > 0 the server pings the client at this interval; otherwise it stays silent. */
    @Volatile private var serverPingIntervalMs = 0L
    private val received = CopyOnWriteArrayList<String>()
    private var socketClosed = CompletableDeferred<Unit>()

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            webSocket("/hub") {
                try {
                    val pinger = launch {
                        while (serverPingIntervalMs > 0) {
                            delay(serverPingIntervalMs)
                            send(Frame.Text("{\"type\":6}$rs"))
                        }
                    }
                    for (frame in incoming) {
                        if (frame !is Frame.Text) continue
                        val text = frame.readText()
                        received += text
                        if (text.contains("\"protocol\"")) {
                            when (handshake) {
                                Handshake.OK -> send(Frame.Text("{}$rs"))
                                Handshake.ERROR -> send(Frame.Text("{\"error\":\"Protocol not supported\"}$rs"))
                                Handshake.NONE -> Unit
                            }
                        }
                    }
                    pinger.cancel()
                } finally {
                    socketClosed.complete(Unit)
                }
            }
        }
    }.also { it.start(wait = false) }

    private val url = "http://127.0.0.1:${runBlocking { server.engine.resolvedConnectors().first().port }}/hub"

    private val client = HttpClient(OkHttp) { install(ClientWebSockets) }

    private fun connection(
        keepAliveMs: Long = 0,
        serverTimeoutMs: Long = 0,
        handshakeTimeoutMs: Long = 2_000,
        baseUrl: String = url,
        accessToken: String? = null,
        logger: SignalRLogger = NoOpSignalRLogger,
    ) = HubConnectionImpl(
        client = client,
        baseUrl = baseUrl,
        connectionHeaders = emptyMap(),
        accessTokenProvider = accessToken?.let { token -> { token } },
        skipNegotiate = true,
        logger = logger,
        handshakeTimeoutMs = handshakeTimeoutMs,
        keepAliveIntervalMs = keepAliveMs,
        serverTimeoutMs = serverTimeoutMs,
    )

    @After
    fun tearDown() {
        client.close()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private fun pingsReceived() = received.count { it == SignalRProtocol.PING_MESSAGE }

    @Test
    fun keepAliveSendsPingsWhileIdle() = runBlocking {
        val connection = connection(keepAliveMs = 100)
        connection.connect()

        delay(550)

        assertTrue("expected several pings, got ${pingsReceived()}", pingsReceived() >= 3)
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun keepAliveIsSkippedWhileTheClientIsSending() = runBlocking {
        val connection = connection(keepAliveMs = 200)
        connection.connect()

        repeat(8) {
            connection.send("Echo", it)
            delay(50)
        }

        assertEquals(0, pingsReceived())
        connection.disconnect()
    }

    @Test
    fun silentServerTimesOutAndClosesTheSocket() = runBlocking {
        val connection = connection(serverTimeoutMs = 300)
        val error = async { connection.errorFlow.first() }
        connection.connect()

        withTimeout(3_000) {
            connection.connectionState.first { it == HubConnectionState.DISCONNECTED }
            socketClosed.await()
        }
        val timeout = withTimeout(1_000) { error.await() }
        assertTrue("got $timeout", timeout is ServerTimeoutException)
    }

    @Test
    fun serverPingsKeepTheConnectionAlive() = runBlocking {
        serverPingIntervalMs = 100
        val connection = connection(serverTimeoutMs = 400)
        connection.connect()

        delay(1_200)

        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun handshakeErrorFailsConnectAndClosesTheSocket() = runBlocking {
        handshake = Handshake.ERROR
        val connection = connection()

        val error = assertThrows(IllegalStateException::class.java) { runBlocking { connection.connect() } }

        assertTrue(error.message!!.contains("Protocol not supported"))
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        withTimeout(2_000) { socketClosed.await() }
    }

    @Test
    fun handshakeTimeoutFailsConnectAndClosesTheSocket() = runBlocking {
        handshake = Handshake.NONE
        val connection = connection(handshakeTimeoutMs = 200)

        val error = assertThrows(IllegalStateException::class.java) { runBlocking { connection.connect() } }

        assertTrue(error.message!!.contains("200ms"))
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        withTimeout(2_000) { socketClosed.await() }
    }

    @Test
    fun connectCanBeRetriedAfterAFailedHandshake() = runBlocking {
        handshake = Handshake.ERROR
        val connection = connection()
        runCatching { connection.connect() }
        withTimeout(2_000) { socketClosed.await() }

        handshake = Handshake.OK
        socketClosed = CompletableDeferred()
        connection.connect()

        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun closeDisconnectsReleasesTheClientAndForbidsReconnect() = runBlocking {
        val connection = connection()
        connection.connect()

        connection.close()

        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        withTimeout(2_000) {
            socketClosed.await()
            client.coroutineContext.job.join()
        }
        assertFalse(client.coroutineContext.job.isActive)
        assertThrows(IllegalStateException::class.java) { runBlocking { connection.connect() } }
        connection.close() // a second close is harmless
    }

    @Test
    fun secretsAndPayloadsAreLoggedOnlyAtVerbose() = runBlocking {
        val logger = RecordingSignalRLogger()
        val connection = connection(
            baseUrl = "$url?tenant=secret-query",
            accessToken = "secret-token",
            logger = logger,
        )
        connection.connect()
        connection.send("Echo", "secret-arg")
        connection.send("""{"H":"chathub","M":"Echo","A":["secret-envelope-arg"]}""")
        connection.disconnect()

        val secrets = listOf("secret-query", "secret-token", "secret-arg", "secret-envelope-arg")
        logger.assertNoneBelowVerbose(secrets)
        // The capture works: the same secrets do reach VERBOSE.
        val verbose = logger.text(SignalRLogLevel.VERBOSE)
        secrets.forEach { assertTrue("'$it' missing from VERBOSE output", verbose.contains(it)) }
        assertTrue(logger.text(SignalRLogLevel.DEBUG).contains("Sending 'chathub.Echo'."))
    }

    @Test
    fun closeWithoutConnectReleasesTheClient() = runBlocking {
        connection().close()

        withTimeout(2_000) { client.coroutineContext.job.join() }
    }
}
