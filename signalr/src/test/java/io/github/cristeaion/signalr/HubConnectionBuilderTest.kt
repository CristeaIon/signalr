package io.github.cristeaion.signalr

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test

class HubConnectionBuilderTest {

    @Test
    fun testBuilderCreation() {
        val connection = HubConnectionBuilder.create("https://example.com/hub")
            .withHeader("HeaderKey", "HeaderValue")
            .withHeaders(mapOf("Header2" to "Val2"))
            .withAccessTokenProvider { "test-token" }
            .withHandshakeTimeout(5000)
            .shouldSkipNegotiate(true)
            .build()

        assertNotNull(connection)
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun testBuilderWithCustomLogger() {
        val loggedMessages = mutableListOf<String>()
        val customLogger = object : SignalRLogger {
            override fun log(
                level: SignalRLogLevel,
                tag: String,
                message: String,
                throwable: Throwable?
            ) {
                loggedMessages.add("[$level][$tag] $message")
            }
        }

        val connection = HubConnectionBuilder.create("https://example.com/hub")
            .withLogger(customLogger)
            .withLogLevel(SignalRLogLevel.DEBUG)
            .shouldSkipNegotiate(true)
            .build()

        assertNotNull(connection)
    }

    @Test
    fun testHubConnectionStateValues() {
        val states = HubConnectionState.values()
        assertEquals(3, states.size)
        assertEquals(HubConnectionState.DISCONNECTED, HubConnectionState.valueOf("DISCONNECTED"))
        assertEquals(HubConnectionState.CONNECTING, HubConnectionState.valueOf("CONNECTING"))
        assertEquals(HubConnectionState.CONNECTED, HubConnectionState.valueOf("CONNECTED"))
    }
}
