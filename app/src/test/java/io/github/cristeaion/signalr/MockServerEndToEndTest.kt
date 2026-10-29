package io.github.cristeaion.signalr

import io.github.cristeaion.signalr.mockserver.MockServer
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test

/** Drives the real client and auth service over real sockets against an in-process mock server. */
class MockServerEndToEndTest {

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

    private suspend fun loginAs(user: String): String {
        val result = DemoAuthService(baseUrl = "$host/api/login").login(user, "111111")
        assertTrue("Login failed: $result", result is LoginResult.Success)
        return (result as LoginResult.Success).sid
    }

    private fun chatUrl(sid: String) =
        DemoAuthService.updateSessionIdInUrl(DemoServer.SIGNALR_URL.replace(DemoServer.HOST, host), sid)

    private fun buildConnection(url: String) = HubConnectionBuilder.create(url)
        .withLogLevel(SignalRLogLevel.WARN)
        .withHandshakeTimeout(2_000)
        .build()

    private suspend fun connectAs(user: String): HubConnection =
        buildConnection(chatUrl(loginAs(user))).also { it.connect() }

    private suspend fun <T> Channel<T>.next(): T = withTimeout(2_000) { receive() }

    @Test
    fun loginWithWrongPasswordIsRejected() = runBlocking {
        val result = DemoAuthService(baseUrl = "$host/api/login").login("sa@example.com", "wrong")
        assertTrue(result is LoginResult.Error)
        assertTrue((result as LoginResult.Error).errorMessage.contains("Invalid credentials"))
    }

    @Test
    fun loginConnectChatInvokeAndDisconnect() = runBlocking {
        val alice = connectAs("alice@example.com")
        assertEquals(HubConnectionState.CONNECTED, alice.connectionState.value)

        val messages = Channel<Pair<String, String>>(Channel.UNLIMITED)
        val joined = Channel<String>(Channel.UNLIMITED)
        val left = Channel<String>(Channel.UNLIMITED)
        alice.on<String, String>("ReceiveMessage") { user, message -> messages.trySend(user to message) }
        alice.on<String>("UserJoined") { joined.trySend(it) }
        alice.on<String>("UserLeft") { left.trySend(it) }

        alice.send("Send", "hello")
        assertEquals("alice@example.com" to "hello", messages.next())
        val botReply = messages.next()
        assertEquals("ChatBot", botReply.first)
        assertTrue(botReply.second.contains("hello"))

        val history = alice.invoke("GetHistory") as JsonArray
        assertTrue(history.toString().contains("\"message\":\"hello\""))

        val bob = connectAs("bob@example.com")
        assertEquals("bob@example.com", joined.next())
        assertEquals("""["alice@example.com","bob@example.com"]""", alice.invoke("GetOnlineUsers").toString())

        bob.send("Send", "hi alice")
        assertEquals("bob@example.com" to "hi alice", messages.next())
        assertEquals("ChatBot", messages.next().first)

        bob.disconnect()
        assertEquals("bob@example.com", left.next())

        alice.disconnect()
        assertEquals(HubConnectionState.DISCONNECTED, alice.connectionState.value)
    }

    @Test
    fun connectWithUnknownSessionFails() = runBlocking {
        val connection = buildConnection(chatUrl("not-issued-by-mock-auth"))
        val errorScope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val errors = Channel<Throwable>(Channel.UNLIMITED)
        connection.onError(errorScope) { errors.trySend(it) }
        try {
            connection.connect()
            fail("Expected negotiate to be rejected")
        } catch (e: RuntimeException) {
            assertTrue(e.message!!.contains("401"))
        }
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
        assertTrue(errors.next().message!!.contains("401"))
        errorScope.cancel()
    }

    @Test
    fun serverDropMovesClientToDisconnected() = runBlocking {
        val connection = connectAs("sa@example.com")
        server.hub.dropAll()
        withTimeout(2_000) {
            connection.connectionState.first { it == HubConnectionState.DISCONNECTED }
        }
        Unit
    }

    @Test
    fun failedInvokeSurfacesServerError() = runBlocking {
        val connection = connectAs("sa@example.com")
        server.hub.faults.failNextInvoke.set(true)
        try {
            connection.invoke("GetHistory")
            fail("Expected the invocation to fail")
        } catch (e: RuntimeException) {
            assertTrue(e.message!!.contains("Mock failure"))
        }
        connection.disconnect()
    }
}
