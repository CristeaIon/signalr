package io.github.cristeaion.signalr.mockserver

import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.util.concurrent.CompletionStage
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class MockServerTest {

    private val server = MockServer()
    private val http = HttpClient.newHttpClient()
    private var base = ""

    @Before
    fun setUp() {
        base = "http://127.0.0.1:${server.start(0)}"
    }

    @After
    fun tearDown() {
        server.stop()
    }

    private fun get(path: String): HttpResponse<String> =
        http.send(HttpRequest.newBuilder(URI("$base$path")).build(), HttpResponse.BodyHandlers.ofString())

    private fun post(path: String): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI("$base$path")).POST(HttpRequest.BodyPublishers.noBody()).build(),
            HttpResponse.BodyHandlers.ofString()
        )

    private fun login(user: String, password: String): HttpResponse<String> {
        val rq = """[{"action":"Login","args":{"userName":"$user","password":"$password","generateToken":true}}]"""
        return get("/api/login?appid=demo&timestamp=1&rq=${URLEncoder.encode(rq, "UTF-8")}")
    }

    private fun sidOf(response: HttpResponse<String>) =
        Regex("\"sid\":\"([^\"]+)\"").find(response.body())!!.groupValues[1]

    private fun negotiate(sid: String): HttpResponse<String> =
        post("/signalr/negotiate?connectionData=${URLEncoder.encode("""[{"name":"chathub"}]""", "UTF-8")}&sessionId=${URLEncoder.encode(sid, "UTF-8")}")

    private fun tokenOf(response: HttpResponse<String>) =
        Regex("\"ConnectionToken\":\"([^\"]+)\"").find(response.body())!!.groupValues[1]

    /** Collects text frames into a queue so tests can await them without sleeping. */
    private class Frames : WebSocket.Listener {
        val queue = LinkedBlockingQueue<String>()
        private val buffer = StringBuilder()

        override fun onText(webSocket: WebSocket, data: CharSequence, last: Boolean): CompletionStage<*>? {
            buffer.append(data)
            if (last) {
                queue.add(buffer.toString())
                buffer.setLength(0)
            }
            webSocket.request(1)
            return null
        }

        fun next(): String = queue.poll(2, TimeUnit.SECONDS) ?: error("No frame within 2s")
    }

    private fun connect(user: String): Pair<WebSocket, Frames> {
        val token = tokenOf(negotiate(sidOf(login(user, "111111"))))
        val frames = Frames()
        val ws = http.newWebSocketBuilder()
            .buildAsync(URI("${base.replace("http", "ws")}/signalr/connect?transport=webSockets&connectionToken=$token"), frames)
            .get(2, TimeUnit.SECONDS)
        assertTrue(frames.next().contains("\"S\":1"))
        assertEquals(200, get("/signalr/start?connectionToken=$token").statusCode())
        return ws to frames
    }

    @Test
    fun loginWithDemoUserReturnsSid() {
        val response = login("sa@example.com", "111111")
        assertEquals(200, response.statusCode())
        assertTrue(sidOf(response).startsWith("demo~@~"))
    }

    @Test
    fun loginWithWrongPasswordReturnsError() {
        val response = login("sa@example.com", "wrong")
        assertEquals("""[{"action":"Login","error":"Invalid credentials"}]""", response.body())
    }

    @Test
    fun loginWithMalformedRequestReturns400() {
        assertEquals(400, get("/api/login?rq=not-json").statusCode())
        assertEquals(400, get("/api/login").statusCode())
    }

    @Test
    fun negotiateRejectsUnknownSession() {
        assertEquals(401, negotiate("bogus").statusCode())
    }

    @Test
    fun startRejectsUnknownToken() {
        assertEquals(400, get("/signalr/start?connectionToken=bogus").statusCode())
    }

    @Test
    fun rejectNextNegotiateFailsOnlyOnce() {
        val sid = sidOf(login("sa@example.com", "111111"))
        assertEquals(204, post("/mock/control/reject-next-negotiate").statusCode())
        assertEquals(500, negotiate(sid).statusCode())
        assertEquals(200, negotiate(sid).statusCode())
    }

    @Test
    fun chatMessageIsBroadcastWithBotReplyAndKeptInHistory() {
        val (alice, aliceFrames) = connect("alice@example.com")
        val (_, bobFrames) = connect("bob@example.com")
        assertTrue(aliceFrames.next().contains(""""M":"UserJoined","A":["bob@example.com"]"""))

        alice.sendText("""{"H":"chathub","M":"Send","A":["hello"]}""", true).get()

        for (frames in listOf(aliceFrames, bobFrames)) {
            assertTrue(frames.next().contains(""""M":"ReceiveMessage","A":["alice@example.com","hello"]"""))
            assertTrue(frames.next().contains(""""A":["ChatBot","""))
        }

        alice.sendText("""{"H":"chathub","M":"GetHistory","A":[],"I":"7"}""", true).get()
        val history = aliceFrames.next()
        assertTrue(history.startsWith("""{"I":"7","R":["""))
        assertTrue(history.contains(""""message":"hello""""))

        alice.sendText("""{"H":"chathub","M":"GetOnlineUsers","A":[],"I":"8"}""", true).get()
        assertEquals("""{"I":"8","R":["alice@example.com","bob@example.com"]}""", aliceFrames.next())
    }

    @Test
    fun failNextInvokeAndUnknownMethodReturnErrors() {
        val (ws, frames) = connect("sa@example.com")
        post("/mock/control/fail-next-invoke")
        ws.sendText("""{"H":"chathub","M":"GetHistory","A":[],"I":"1"}""", true).get()
        assertEquals("""{"I":"1","E":"Mock failure"}""", frames.next())

        ws.sendText("""{"H":"chathub","M":"Nope","A":[],"I":"2"}""", true).get()
        assertEquals("""{"I":"2","E":"Method 'chathub.Nope' not found"}""", frames.next())
    }

    @Test
    fun disconnectNotifiesOthersAndDropAllClosesSockets() {
        val (alice, aliceFrames) = connect("alice@example.com")
        val (bob, _) = connect("bob@example.com")
        aliceFrames.next() // bob joined

        bob.sendClose(WebSocket.NORMAL_CLOSURE, "bye").get()
        assertTrue(aliceFrames.next().contains(""""M":"UserLeft","A":["bob@example.com"]"""))

        post("/mock/control/drop-all")
        val deadline = System.currentTimeMillis() + 2_000
        while (!alice.isInputClosed && System.currentTimeMillis() < deadline) Thread.sleep(20)
        assertTrue(alice.isInputClosed)
        assertTrue(server.hub.onlineUsers().isEmpty())
    }
}
