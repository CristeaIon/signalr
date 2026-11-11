package io.github.cristeaion.signalr

import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.readText
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/** What a connection built with the public builder does when the app configures nothing. */
class HubConnectionDefaultsTest {

    private val userAgents = CopyOnWriteArrayList<String?>()

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            webSocket("/hub") {
                userAgents += call.request.headers["User-Agent"]
                for (frame in incoming) {
                    if (frame is Frame.Text && frame.readText().contains("\"protocol\"")) {
                        send(Frame.Text("{}${SignalRProtocol.RECORD_SEPARATOR}"))
                    }
                }
            }
        }
    }.also { it.start(wait = false) }

    private val url = "http://127.0.0.1:${runBlocking { server.engine.resolvedConnectors().first().port }}/hub"

    @After
    fun tearDown() {
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private fun builder() = HubConnectionBuilder.create(url).shouldSkipNegotiate(true)

    @Test
    fun defaultUserAgentNamesOnlyTheLibrary() = runBlocking {
        builder().build().use { it.connect(); it.disconnect() }

        assertEquals(listOf("signalr-kotlin/${BuildConfig.LIBRARY_VERSION}"), userAgents)
    }

    @Test
    fun anAppUserAgentWinsWhateverItsCase() = runBlocking {
        builder().withHeader("user-agent", "MyApp/2.1").build().use { it.connect(); it.disconnect() }

        assertEquals(listOf("MyApp/2.1"), userAgents)
    }

    @Test
    fun defaultLevelIsWarn() = runBlocking {
        val logger = RecordingSignalRLogger()
        builder().withLogger(logger).build().use { connection ->
            connection.connect()
            connection.send("Echo", "hi")
            connection.disconnect()
        }

        assertTrue("nothing below WARN expected, got ${logger.entries}", logger.entries.all { it.level.isAtLeast(SignalRLogLevel.WARN) })
        assertFalse(ConsoleSignalRLogger().isLoggable(SignalRLogLevel.INFO))
        assertTrue(ConsoleSignalRLogger().isLoggable(SignalRLogLevel.WARN))
    }

    @Test
    fun anArgumentWithoutASerializerFailsTheCallInsteadOfSendingToString() = runBlocking {
        class NotSerializable(val x: Int)
        builder().build().use { connection ->
            connection.connect()

            val error = assertThrows(IllegalArgumentException::class.java) {
                runBlocking { connection.send("Echo", NotSerializable(1)) }
            }
            assertTrue(error.message, error.message!!.contains("@Serializable"))
            assertThrows(IllegalArgumentException::class.java) {
                runBlocking { connection.invoke("Echo", NotSerializable(2)) }
            }
            assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        }
    }
}
