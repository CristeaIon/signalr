package io.github.cristeaion.signalr

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test

class ExampleAppHubIntegrationTest {

    @Test
    fun testParseArgsWithEmptyString() {
        assertEquals(0, parseHubArgs("").size)
        assertEquals(0, parseHubArgs("   \n\t  ").size)
    }

    @Test
    fun testParseArgsWithJsonArray() {
        val parsed = parseHubArgs("""["hello", 2, true]""")
        assertEquals(3, parsed.size)
        assertEquals(JsonPrimitive("hello"), parsed[0])
        assertEquals(JsonPrimitive(2), parsed[1])
        assertEquals(JsonPrimitive(true), parsed[2])
    }

    @Test
    fun testParseArgsWithJsonObject() {
        val parsed = parseHubArgs("""{"H":"chathub","M":"Send","A":["hi"]}""")
        assertEquals(1, parsed.size)
        assertTrue(parsed[0].toString().contains("chathub"))
    }

    @Test
    fun testParseArgsWithRawTextFallback() {
        val parsed = parseHubArgs("plain non-json text")
        assertEquals(1, parsed.size)
        assertEquals("plain non-json text", parsed[0])
    }

    @Test
    fun testLogLevelMappingToLogTypes() {
        assertEquals(LogType.ERROR, SignalRLogLevel.ERROR.toLogType())
        assertEquals(LogType.ERROR, SignalRLogLevel.WARN.toLogType())
        assertEquals(LogType.INFO, SignalRLogLevel.INFO.toLogType())
        assertEquals(LogType.INFO, SignalRLogLevel.DEBUG.toLogType())
        assertEquals(LogType.INFO, SignalRLogLevel.VERBOSE.toLogType())
        assertEquals(LogType.INFO, SignalRLogLevel.NONE.toLogType())
    }

    @Test
    fun testDefaultUrlDeclaresOnlyTheChatHub() {
        assertTrue(DemoServer.SIGNALR_URL.startsWith(DemoServer.HOST))
        assertTrue(DemoServer.SIGNALR_URL.contains("""connectionData=[{"name":"chathub"}]"""))
    }

    @Test
    fun testHubConnectionConfigurationAndEventRegistration() = runBlocking {
        val connection = HubConnectionBuilder.create(DemoServer.SIGNALR_URL)
            .withLogLevel(SignalRLogLevel.DEBUG)
            .withHandshakeTimeout(15_000)
            .build()

        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)

        // Same registrations as MainActivity.connectToHub
        val rawSub = connection.on("UserJoined") { }
        val typedSub = connection.on<String, String>("ReceiveMessage") { _, _ -> }
        val testScope = CoroutineScope(Dispatchers.Unconfined + SupervisorJob())
        val errorSub = connection.onError(testScope) { }

        rawSub.cancel()
        typedSub.cancel()
        errorSub.cancel()
        testScope.cancel()
    }

    @Test
    fun testSendMessageWhileDisconnectedFailsGracefully() = runBlocking {
        val connection = HubConnectionBuilder.create(DemoServer.SIGNALR_URL).build()
        try {
            connection.send("Send", *parseHubArgs("""["hi"]"""))
            fail("Expected IllegalStateException when sending while disconnected")
        } catch (e: IllegalStateException) {
            assertTrue(e.message!!.contains("DISCONNECTED"))
        }
    }

    @Test
    fun testInvokeMessageWhileDisconnectedFailsGracefully() = runBlocking {
        val connection = HubConnectionBuilder.create(DemoServer.SIGNALR_URL).build()
        try {
            connection.invoke("GetHistory")
            fail("Expected IllegalStateException when invoking while disconnected")
        } catch (e: IllegalStateException) {
            assertTrue(e.message!!.contains("DISCONNECTED"))
        }
    }

    @Test
    fun testDisconnectWhileDisconnectedIsNoOp() = runBlocking {
        val connection = HubConnectionBuilder.create(DemoServer.SIGNALR_URL).build()
        connection.disconnect()
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }
}
