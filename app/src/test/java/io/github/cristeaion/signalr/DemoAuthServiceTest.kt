package io.github.cristeaion.signalr

import org.junit.Assert.assertEquals
import org.junit.Test

class DemoAuthServiceTest {

    private val service = DemoAuthService()

    @Test
    fun testExtractSidDirect() {
        val json = """[{"action":"Login","sid":"3687_4c861e69-d44b-44c4-a9ff-13aee833081d"}]"""
        val sid = service.extractSidFromResponse(json)
        assertEquals("3687_4c861e69-d44b-44c4-a9ff-13aee833081d", sid)
    }

    @Test
    fun testExtractSidInResult() {
        val json = """[{"action":"Login","result":{"sessionId":"session_abc_123"}}]"""
        val sid = service.extractSidFromResponse(json)
        assertEquals("session_abc_123", sid)
    }

    @Test
    fun testExtractSidRegexFallback() {
        val rawResponse = """some html or non-standard json with "sessionId":"regex_sid_999" in payload"""
        val sid = service.extractSidFromResponse(rawResponse)
        assertEquals("regex_sid_999", sid)
    }

    @Test
    fun testUpdateSessionIdInUrl() {
        val url = "http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]&sessionId=OLD_SID_123"
        val updated = DemoAuthService.updateSessionIdInUrl(url, "NEW_SID_456")
        assertEquals("http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]&sessionId=NEW_SID_456", updated)
    }

    @Test
    fun testAppendSessionIdToUrl() {
        val url = "http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]"
        val updated = DemoAuthService.updateSessionIdInUrl(url, "NEW_SID_456")
        assertEquals("http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"}]&sessionId=NEW_SID_456", updated)
    }
}
