package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.engine.mock.respondError
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import java.net.URLDecoder

class DemoAuthIntegrationTest {

    @Test
    fun testLoginSuccessWithStandardSid() = runBlocking {
        var requestedUrl: String? = null
        val mockEngine = MockEngine { request ->
            requestedUrl = request.url.toString()
            respond(
                content = """[{"action":"Login","sid":"3687_4c861e69-d44b-44c4-a9ff-13aee833081d"}]""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("user@test.com", "secret123")

        assertTrue(result is LoginResult.Success)
        val success = result as LoginResult.Success
        assertEquals("3687_4c861e69-d44b-44c4-a9ff-13aee833081d", success.sid)

        // Verify request parameters sent to the auth service
        val url = requestedUrl!!
        assertTrue(url.startsWith(DemoServer.AUTH_URL))
        assertTrue(url.contains("appid=demo-app"))
        assertTrue(url.contains("timestamp="))

        val rqParam = url.substringAfter("rq=").substringBefore("&")
        val decodedRq = URLDecoder.decode(rqParam, "UTF-8")
        assertTrue(decodedRq.contains(""""userName":"user@test.com""""))
        assertTrue(decodedRq.contains(""""password":"secret123""""))
        assertTrue(decodedRq.contains(""""action":"Login""""))
        assertTrue(decodedRq.contains(""""generateToken":true"""))
    }

    @Test
    fun testLoginSuccessWithNestedResultSessionId() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """[{"action":"Login","result":{"sessionId":"session_xyz_789"}}]""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("trader1", "pass123")

        assertTrue(result is LoginResult.Success)
        assertEquals("session_xyz_789", (result as LoginResult.Success).sid)
    }

    @Test
    fun testLoginSuccessWithNestedResultToken() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """[{"action":"Login","result":{"Token":"token_capital_456"}}]""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("trader1", "pass123")

        assertTrue(result is LoginResult.Success)
        assertEquals("token_capital_456", (result as LoginResult.Success).sid)
    }

    @Test
    fun testLoginSuccessWithJsonObjectResponse() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """{"action":"Login","sessionId":"root_session_abc"}""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("trader1", "pass123")

        assertTrue(result is LoginResult.Success)
        assertEquals("root_session_abc", (result as LoginResult.Success).sid)
    }

    @Test
    fun testLoginSuccessWithRegexFallback() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """<html><body>Auth OK: {"sessionId":"regex_extracted_sid_111"}</body></html>""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "text/html")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("trader1", "pass123")

        assertTrue(result is LoginResult.Success)
        assertEquals("regex_extracted_sid_111", (result as LoginResult.Success).sid)
    }

    @Test
    fun testLoginFailureMissingSidInResponse() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """[{"action":"Login","error":"Invalid credentials"}]""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("invalid_user", "wrong_pass")

        assertTrue(result is LoginResult.Error)
        val error = result as LoginResult.Error
        assertTrue(error.errorMessage.contains("could not extract session ID"))
        assertTrue(error.errorMessage.contains("Invalid credentials"))
    }

    @Test
    fun testLoginFailureHttpServerError() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = "500 Internal Server Error",
                status = HttpStatusCode.InternalServerError,
                headers = headersOf(HttpHeaders.ContentType, "text/plain")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("user", "pass")

        assertTrue(result is LoginResult.Error)
        val error = result as LoginResult.Error
        assertTrue(error.errorMessage.contains("could not extract session ID") || error.errorMessage.contains("500"))
    }

    @Test
    fun testLoginFailureNetworkException() = runBlocking {
        val mockEngine = MockEngine { _ ->
            throw java.io.IOException("Connection reset by peer")
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val result = authService.login("user", "pass")

        assertTrue(result is LoginResult.Error)
        val error = result as LoginResult.Error
        assertTrue(error.errorMessage.startsWith("Network/Login error:"))
        assertTrue(error.errorMessage.contains("Connection reset by peer"))
    }

    @Test
    fun testLoginFlowUpdatesSignalRUrlForDashboard() = runBlocking {
        val mockEngine = MockEngine { _ ->
            respond(
                content = """[{"action":"Login","sid":"3687~@~9999999_extracted-session-id"}]""",
                status = HttpStatusCode.OK,
                headers = headersOf(HttpHeaders.ContentType, "application/json")
            )
        }

        val authService = DemoAuthService(client = HttpClient(mockEngine))
        val loginResult = authService.login("test@example.com", "pass")

        assertTrue(loginResult is LoginResult.Success)
        val sid = (loginResult as LoginResult.Success).sid

        var signalRUrl = "http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"},{\"name\":\"quoteshub\"}]&sessionId=old_session_id"
        signalRUrl = DemoAuthService.updateSessionIdInUrl(signalRUrl, sid)

        assertEquals(
            "http://127.0.0.1:8080/signalr/negotiate?connectionData=[{\"name\":\"userinfohub\"},{\"name\":\"quoteshub\"}]&sessionId=3687~@~9999999_extracted-session-id",
            signalRUrl
        )
    }
}
