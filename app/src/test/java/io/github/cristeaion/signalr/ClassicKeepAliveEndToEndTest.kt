package io.github.cristeaion.signalr

import io.github.cristeaion.signalr.mockserver.MockServer
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test

/**
 * Classic servers announce `KeepAliveTimeout` in negotiate and send `{}` keep-alives. The mock
 * server here keeps alive every 150ms, so it announces a 300ms timeout.
 */
class ClassicKeepAliveEndToEndTest {

    private val server = MockServer(keepAliveIntervalMs = 150)
    private var host = ""

    @Before
    fun setUp() {
        host = "http://127.0.0.1:${server.start(0)}"
    }

    @After
    fun tearDown() {
        server.stop()
    }

    private suspend fun connect(): HubConnection {
        val sid = (DemoAuthService(baseUrl = "$host/api/login").login("sa@example.com", "111111") as LoginResult.Success).sid
        val url = DemoAuthService.updateSessionIdInUrl(DemoServer.SIGNALR_URL.replace(DemoServer.HOST, host), sid)
        return HubConnectionBuilder.create(url)
            .withLogLevel(SignalRLogLevel.WARN)
            .withHandshakeTimeout(2_000)
            .build()
            .also { it.connect() }
    }

    @Test
    fun keepAlivesHoldAnIdleConnectionOpen() = runBlocking {
        val connection = connect()

        delay(1_000) // more than three announced timeouts

        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.close()
    }

    @Test
    fun silentServerIsDetectedThroughItsAnnouncedTimeout() = runBlocking {
        val connection = connect()
        val error = async { connection.errorFlow.first() }

        server.hub.faults.silent.set(true)

        withTimeout(2_000) { connection.connectionState.first { it == HubConnectionState.DISCONNECTED } }
        val timeout = withTimeout(1_000) { error.await() }
        assertTrue("got $timeout", timeout is ServerTimeoutException)
        assertEquals(300L, (timeout as ServerTimeoutException).timeoutMs)
        connection.close()
    }
}
