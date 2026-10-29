package io.github.cristeaion.signalr

import kotlinx.serialization.Serializable
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

@Serializable
data class TestUser(val id: Int, val name: String)

class SignalRProtocolTest {

    @Test
    fun testHandshakeMessage() {
        val handshake = SignalRProtocol.createHandshakeMessage()
        assertEquals("{\"protocol\":\"json\",\"version\":1}\u001e", handshake)
    }

    @Test
    fun testParseFrames() {
        val payload = "{\"type\":6}\u001e{\"type\":1,\"target\":\"ReceiveMessage\",\"arguments\":[\"test\"]}\u001e"
        val frames = SignalRProtocol.parseFrames(payload)
        assertEquals(2, frames.size)
        assertEquals("{\"type\":6}", frames[0])
        assertEquals("{\"type\":1,\"target\":\"ReceiveMessage\",\"arguments\":[\"test\"]}", frames[1])
    }

    @Test
    fun testParseEmptyFrames() {
        val frames = SignalRProtocol.parseFrames("")
        assertTrue(frames.isEmpty())
    }

    @Test
    fun testCreateInvocationMessage() {
        val message = SignalRProtocol.createInvocationMessage("SendMessage", "1", arrayOf("AndroidDevice", "Hello"))
        assertTrue(message.endsWith("\u001e"))
        assertTrue(message.contains("\"target\":\"SendMessage\""))
        assertTrue(message.contains("\"invocationId\":\"1\""))
        assertTrue(message.contains("\"arguments\":[\"AndroidDevice\",\"Hello\"]"))
    }

    @Test
    fun testCreateInvocationMessageWithSerializableObject() {
        val user = TestUser(10, "Alice")
        val message = SignalRProtocol.createInvocationMessage("AddUser", null, arrayOf(user, 123, true))
        assertTrue(message.endsWith("\u001e"))
        assertTrue(message.contains("\"target\":\"AddUser\""))
        assertTrue(message.contains("\"arguments\":[{\"id\":10,\"name\":\"Alice\"},123,true]"))
    }

    @Test
    fun testClassicInvocationMessageHasNoRecordSeparator() {
        val message = SignalRProtocol.createClassicInvocationMessage(
            "quoteshub",
            "SubscribeWithSnapshot",
            "7",
            arrayOf(listOf("EURUSD"))
        )

        assertFalse(message.contains(SignalRProtocol.RECORD_SEPARATOR))
        assertEquals("{\"H\":\"quoteshub\",\"M\":\"SubscribeWithSnapshot\",\"A\":[[\"EURUSD\"]],\"I\":\"7\"}", message)
    }

    @Test
    fun testClassicInvocationMessageOmitsIdWhenSending() {
        val message = SignalRProtocol.createClassicInvocationMessage("quoteshub", "Ping", null, arrayOf())

        assertEquals("{\"H\":\"quoteshub\",\"M\":\"Ping\",\"A\":[]}", message)
    }

    @Test
    fun testWithClassicInvocationIdStampsAndStripsSeparator() {
        val envelope = "{\"H\":\"quoteshub\",\"M\":\"GetSnapshot\",\"A\":[[\"EURUSD\"]],\"I\":13}" + SignalRProtocol.RECORD_SEPARATOR
        val stamped = SignalRProtocol.withClassicInvocationId(envelope, "4")

        assertFalse(stamped.contains(SignalRProtocol.RECORD_SEPARATOR))
        assertTrue(stamped.contains("\"I\":\"4\""))
    }

    @Test
    fun testAsClassicEnvelopeAcceptsParsedJsonObject() {
        val parsed = SignalRProtocol.json.parseToJsonElement("{\"H\":\"quoteshub\",\"M\":\"GetSnapshot\",\"A\":[[\"EURUSD\"]],\"I\":13}")
        val envelope = SignalRProtocol.asClassicEnvelope(parsed)

        assertNotNull(envelope)
        assertEquals(
            "{\"H\":\"quoteshub\",\"M\":\"GetSnapshot\",\"A\":[[\"EURUSD\"]],\"I\":\"5\"}",
            SignalRProtocol.renderClassicEnvelope(envelope!!, "5")
        )
    }

    @Test
    fun testAsClassicEnvelopeRejectsNonEnvelopes() {
        assertNull(SignalRProtocol.asClassicEnvelope("[\"EURUSD\"]"))
        assertNull(SignalRProtocol.asClassicEnvelope("GetSnapshot"))
        assertNull(SignalRProtocol.asClassicEnvelope(SignalRProtocol.json.parseToJsonElement("{\"foo\":1}")))
    }

    @Test
    fun testIsClassicEnvelope() {
        assertTrue(SignalRProtocol.isClassicEnvelope("{\"H\":\"quoteshub\",\"M\":\"GetSnapshot\"}"))
        assertFalse(SignalRProtocol.isClassicEnvelope("[\"EURUSD\"]"))
    }

    @Test
    fun testNegotiateResponseParsing() {
        val jsonStr = """{"connectionId":"xyz123","negotiateVersion":1,"availableTransports":[{"transport":"WebSockets","transferFormats":["Text","Binary"]}]}"""
        val response = SignalRProtocol.json.decodeFromString<NegotiateResponse>(jsonStr)
        assertEquals("xyz123", response.connectionId)
        assertEquals(1, response.negotiateVersion)
        assertEquals(1, response.availableTransports?.size)
        assertEquals("WebSockets", response.availableTransports?.first()?.transport)
    }
}
