package io.github.cristeaion.signalr

import io.github.cristeaion.signalr.mockserver.MockServer
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList

/** Classic SignalR 2.x connect paths against the mock server, observed from both ends. */
class ClassicConnectEndToEndTest {

    private val server = MockServer()
    private var host = ""

    @Before
    fun setUp() {
        host = "http://127.0.0.1:${server.start(0)}"
    }

    @After
    fun tearDown() {
        server.stop()
    }

    private class Recorder : SignalRLogger {
        val entries = CopyOnWriteArrayList<Pair<SignalRLogLevel, String>>()
        override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
            val causes = generateSequence(throwable) { it.cause }.joinToString(" | ") { it.toString() }
            entries += level to if (causes.isEmpty()) message else "$message | $causes"
        }
    }

    private suspend fun login(): String =
        (DemoAuthService(baseUrl = "$host/api/login").login("sa@example.com", "111111") as LoginResult.Success).sid

    private fun build(sid: String, logger: SignalRLogger? = null): HubConnection {
        val url = DemoAuthService.updateSessionIdInUrl(DemoServer.SIGNALR_URL.replace(DemoServer.HOST, host), sid)
        return HubConnectionBuilder.create(url)
            .apply { if (logger != null) withLogger(logger) }
            .withLogLevel(if (logger != null) SignalRLogLevel.VERBOSE else SignalRLogLevel.WARN)
            .withHandshakeTimeout(2_000)
            .build()
    }

    @Test
    fun failedClassicStartClosesTheOpenedSocket() = runBlocking {
        val connection = build(login())
        server.hub.faults.rejectNextStart.set(true)

        try {
            connection.connect()
            fail("Expected /start to be rejected")
        } catch (e: RuntimeException) {
            // /start runs only once the WebSocket is open, so this proves a socket was opened.
            assertTrue(e.message!!, e.message!!.contains("Classic start failed"))
        }

        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        withTimeout(2_000) {
            while (server.hub.openSocketCount() > 0) delay(20)
        }
        connection.close()
    }

    @Test
    fun classicSessionLogsSecretsAndPayloadsOnlyAtVerbose() = runBlocking {
        val sid = login()
        val logger = Recorder()
        val connection = build(sid, logger)
        val received = Channel<String>(Channel.UNLIMITED)
        connection.on<String, String>("ReceiveMessage") { _, message -> received.trySend(message) }

        connection.connect()
        connection.send("Send", "secret-chat-message")
        withTimeout(2_000) { received.receive() }
        connection.invoke("GetHistory") // the reply carries the message back
        connection.disconnect()
        connection.close()

        val verbose = logger.entries.filter { it.first == SignalRLogLevel.VERBOSE }.joinToString("\n") { it.second }
        val connectionToken = Regex("Negotiated connection token: (\\S+)").find(verbose)?.groupValues?.get(1)
            ?: error("connection token never logged at VERBOSE:\n$verbose")
        // The sid's random part survives any URL encoding, so search for that.
        val secrets = listOf(sid.substringAfterLast('_'), connectionToken, "secret-chat-message")
        secrets.forEach { assertTrue("'$it' missing from VERBOSE output", verbose.contains(it)) }

        val leaks = logger.entries
            .filter { it.first != SignalRLogLevel.VERBOSE }
            .filter { (_, text) -> secrets.any { text.contains(it) } }
        assertTrue(
            "Secrets logged above VERBOSE:\n" + leaks.joinToString("\n") { "[${it.first}] ${it.second}" },
            leaks.isEmpty()
        )
    }
}
