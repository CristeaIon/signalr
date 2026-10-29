package io.github.cristeaion.signalr

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test

class HubConnectionTest {

    @Test
    fun testGetNegotiateUrlWithHubUrl() {
        val baseUrl = "https://example.com/chathub"
        val negotiateUrl = getNegotiateUrl(baseUrl)
        assertEquals("https://example.com/chathub/negotiate", negotiateUrl)
    }

    @Test
    fun testGetNegotiateUrlWithAlreadyNegotiatePath() {
        val baseUrl = "https://prpsignalr.easy-markets.com/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]&sessionId=123"
        val negotiateUrl = getNegotiateUrl(baseUrl)
        assertEquals("https://prpsignalr.easy-markets.com/signalr/negotiate?connectionData=%5B%7B%22name%22%3A%22userinfohub%22%7D%5D&sessionId=123", negotiateUrl)
    }

    @Test
    fun testGetNegotiateUrlWithTrailingSlash() {
        val baseUrl = "https://example.com/chathub/"
        val negotiateUrl = getNegotiateUrl(baseUrl)
        assertEquals("https://example.com/chathub/negotiate", negotiateUrl)
    }

    @Test
    fun testGetNegotiateUrlWithWsScheme() {
        val baseUrl = "wss://example.com/chathub"
        val negotiateUrl = getNegotiateUrl(baseUrl)
        assertEquals("https://example.com/chathub/negotiate", negotiateUrl)
    }

    @Test
    fun testBuildWebSocketUrlForClassicSignalR() {
        val baseUrl = "https://prpsignalr.easy-markets.com/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]&sessionId=3687_4c861e69"
        val response = NegotiateResponse(
            classicConnectionToken = "token_abc_123",
            classicConnectionId = "id_xyz_789"
        )
        val wsUrl = buildWebSocketUrl(baseUrl, response, null)

        assertTrue(wsUrl.startsWith("wss://prpsignalr.easy-markets.com/signalr/connect?"))
        assertFalse(wsUrl.contains("/negotiate"))
        assertTrue(wsUrl.contains("connectionToken=token_abc_123"))
        assertTrue(wsUrl.contains("transport=webSockets"))
        assertTrue(wsUrl.contains("sessionId=3687_4c861e69"))
    }

    @Test
    fun testBuildWebSocketUrlForAspNetCoreSignalR() {
        val baseUrl = "https://example.com/chathub"
        val response = NegotiateResponse(
            connectionToken = "core_token_456",
            connectionId = "core_id_123"
        )
        val wsUrl = buildWebSocketUrl(baseUrl, response, "bearer_token_789")

        assertEquals("wss://example.com/chathub?id=core_token_456&access_token=bearer_token_789", wsUrl)
    }

    @Test
    fun testBuildWebSocketUrlForAspNetCoreSignalRWithNegotiatePath() {
        val baseUrl = "https://example.com/chathub/negotiate"
        val response = NegotiateResponse(
            connectionToken = "core_token_456"
        )
        val wsUrl = buildWebSocketUrl(baseUrl, response, null)

        assertEquals("wss://example.com/chathub?id=core_token_456", wsUrl)
    }

    @Test
    fun testBuildWebSocketUrlSkipNegotiate() {
        val baseUrl = "https://example.com/chathub"
        val wsUrl = buildWebSocketUrl(baseUrl, null, null)

        assertEquals("wss://example.com/chathub", wsUrl)
    }

    @Test
    fun testGetStartUrlForClassicSignalR() {
        val startUrl = getStartUrl(
            "https://prpsignalr.easy-markets.com/signalr/negotiate?connectionData=[{\"name\":\"quoteshub\"}]&sessionId=123",
            "token_abc_123"
        )

        assertTrue(startUrl.startsWith("https://prpsignalr.easy-markets.com/signalr/start?"))
        assertFalse(startUrl.contains("/negotiate"))
        assertTrue(startUrl.contains("connectionToken=token_abc_123"))
        assertTrue(startUrl.contains("transport=webSockets"))
        assertTrue(startUrl.contains("clientProtocol=1.5"))
        assertTrue(startUrl.contains("sessionId=123"))
    }

    @Test
    fun testExtractClassicHubNames() {
        val hubs = extractClassicHubNames(
            "https://prpsignalr.easy-markets.com/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"},{\"name\":\"quoteshub\"}]&sessionId=123"
        )

        assertEquals(listOf("userinfohub", "quoteshub"), hubs)
    }

    @Test
    fun testExtractClassicHubNamesFromEncodedConnectionData() {
        val hubs = extractClassicHubNames(
            "https://example.com/signalr/negotiate?connectionData=%5B%7B%22name%22%3A%22quoteshub%22%7D%5D"
        )

        assertEquals(listOf("quoteshub"), hubs)
    }

    @Test
    fun testIsClassicSignalRUrl() {
        assertTrue(isClassicSignalRUrl("https://example.com/signalr/negotiate"))
        assertTrue(isClassicSignalRUrl("https://example.com/hub?connectionData=[]"))
        assertFalse(isClassicSignalRUrl("https://example.com/chathub"))
    }

    @Test
    fun testErrorFlowIsExposed() {
        val connection = HubConnectionBuilder.create("https://example.com/chathub")
            .shouldSkipNegotiate(true)
            .build()

        assertNotNull(connection.errorFlow)
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun testSubscriptionCancelStopsHandler() {
        val connection = HubConnectionBuilder.create("https://example.com/chathub")
            .shouldSkipNegotiate(true)
            .build()

        val subscription = connection.on("ReceiveMessage") { }
        subscription.cancel()

        // Cancelling twice must stay harmless.
        subscription.cancel()
    }

    @Test
    fun testSendBeforeConnectFails() {
        val connection = HubConnectionBuilder.create("https://example.com/chathub")
            .shouldSkipNegotiate(true)
            .build()

        val error = assertThrows(IllegalStateException::class.java) {
            runBlocking { connection.send("Ping") }
        }
        assertTrue(error.message!!.contains("DISCONNECTED"))
    }

    @Test
    fun testEventsFlowAndOnFlowExposed() {
        val connection = HubConnectionBuilder.create("https://example.com/chathub")
            .shouldSkipNegotiate(true)
            .build()

        assertNotNull(connection.eventsFlow)
        assertNotNull(connection.onFlow("OnMidRateUpdate"))
    }

    @Test
    fun testDecodeArgNullAsNullable() {
        val list = listOf<JsonElement>(JsonNull)
        val value: Long? = list.decodeArg<Long?>(0)
        assertNull(value)
    }

    @Test
    fun testDecodeArgNullAsNonNullableDefault() {
        val list = listOf<JsonElement>(JsonNull)
        val value: Long = list.decodeArg<Long>(0)
        assertEquals(0L, value)
    }
}
