package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.websocket.WebSockets as ClientWebSockets
import io.ktor.server.application.install
import io.ktor.server.cio.CIO
import io.ktor.server.engine.embeddedServer
import io.ktor.server.routing.routing
import io.ktor.server.websocket.DefaultWebSocketServerSession
import io.ktor.server.websocket.WebSockets
import io.ktor.server.websocket.webSocket
import io.ktor.websocket.Frame
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * invoke() failure modes and timeouts, handler dispatch, and concurrent connect() calls, over
 * real sockets against a minimal ASP.NET Core-style hub. The hub answers `Echo(x)` with `x`
 * after [replyDelayMs], never answers `Never()`, and pushes whatever the test sends via [current].
 */
class HubConnectionCallsTest {

    private val rs = SignalRProtocol.RECORD_SEPARATOR

    @Volatile private var handshakeDelayMs = 0L
    @Volatile private var handshakeError: String? = null
    @Volatile private var replyDelayMs = 0L
    private val socketsOpened = AtomicInteger(0)
    @Volatile private var current: DefaultWebSocketServerSession? = null

    private val server = embeddedServer(CIO, port = 0, host = "127.0.0.1") {
        install(WebSockets)
        routing {
            webSocket("/hub") {
                socketsOpened.incrementAndGet()
                for (frame in incoming) {
                    if (frame !is Frame.Text) continue
                    for (text in SignalRProtocol.parseFrames(frame.readText())) {
                        if (text.contains("\"protocol\"")) {
                            delay(handshakeDelayMs)
                            val error = handshakeError
                            if (error != null) {
                                send(Frame.Text("{\"error\":\"$error\"}$rs"))
                            } else {
                                send(Frame.Text("{}$rs"))
                                current = this
                            }
                            continue
                        }
                        val msg = SignalRProtocol.json.decodeFromString<HubMessageJson>(text)
                        val id = msg.invocationId ?: continue
                        if (msg.target == "Echo") {
                            val arg = msg.arguments!!.first()
                            launch {
                                delay(replyDelayMs)
                                send(Frame.Text("{\"type\":3,\"invocationId\":\"$id\",\"result\":$arg}$rs"))
                            }
                        }
                    }
                }
            }
        }
    }.also { it.start(wait = false) }

    private val url = "http://127.0.0.1:${runBlocking { server.engine.resolvedConnectors().first().port }}/hub"

    private val client = HttpClient(OkHttp) { install(ClientWebSockets) }

    private fun connection(
        policy: RetryPolicy? = null,
        invocationTimeoutMs: Long = 0,
        serverTimeoutMs: Long = 0,
    ) = HubConnectionImpl(
        client = client,
        baseUrl = url,
        connectionHeaders = emptyMap(),
        accessTokenProvider = null,
        skipNegotiate = true,
        logger = NoOpSignalRLogger,
        handshakeTimeoutMs = 2_000,
        keepAliveIntervalMs = 0,
        serverTimeoutMs = serverTimeoutMs,
        retryPolicy = policy,
        invocationTimeoutMs = invocationTimeoutMs,
    )

    @After
    fun tearDown() {
        client.close()
        server.stop(gracePeriodMillis = 0, timeoutMillis = 1_000)
    }

    private suspend fun push(target: String, arg: Int) =
        current!!.send(Frame.Text("{\"type\":1,\"target\":\"$target\",\"arguments\":[$arg]}$rs"))

    /** Runs [block], expecting it to throw [T] - and not a CancellationException posing as one. */
    private suspend inline fun <reified T : Throwable> expect(crossinline block: suspend () -> Unit): T {
        try {
            block()
        } catch (ce: CancellationException) {
            fail("expected ${T::class.simpleName}, got a CancellationException: $ce")
        } catch (t: Throwable) {
            if (t is T) return t
            fail("expected ${T::class.simpleName}, got $t")
        }
        fail("expected ${T::class.simpleName}, nothing was thrown")
        throw AssertionError()
    }

    // --- pending invokes fail with ConnectionLostException ---

    @Test
    fun pendingInvokeFailsWithConnectionLostWhenTheServerDrops() = runBlocking {
        val connection = connection()
        connection.connect()
        val call = async { expect<ConnectionLostException> { connection.invoke("Never") } }
        delay(100)

        current!!.close()

        val error = withTimeout(3_000) { call.await() }
        assertTrue(error.message, error.message!!.contains("lost"))
        assertTrue("the caller's coroutine must stay active", isActive)
    }

    @Test
    fun pendingInvokeFailsWithConnectionLostOnDisconnect() = runBlocking {
        val connection = connection()
        connection.connect()
        val call = async { expect<ConnectionLostException> { connection.invoke("Never") } }
        delay(100)

        connection.disconnect()

        val error = withTimeout(3_000) { call.await() }
        assertTrue(error.message, error.message!!.contains("closed"))
    }

    @Test
    fun pendingInvokeFailsWithConnectionLostWhenReconnecting() = runBlocking {
        val connection = connection(policy = RetryPolicy { 50L })
        connection.connect()
        val call = async { expect<ConnectionLostException> { connection.invoke("Never") } }
        delay(100)

        current!!.close()

        withTimeout(3_000) { call.await() }
        withTimeout(3_000) { connection.connectionState.first { it == HubConnectionState.CONNECTED } }
        connection.disconnect()
    }

    @Test
    fun pendingInvokeFailsWithConnectionLostOnServerTimeout() = runBlocking {
        val connection = connection(serverTimeoutMs = 300)
        connection.connect()

        val error = withTimeout(3_000) { expect<ConnectionLostException> { connection.invoke("Never") } }
        // Coroutine stack-trace recovery (on in tests) may wrap the exception in a copy, so search the chain.
        val chain = generateSequence<Throwable>(error) { it.cause }.toList()
        assertTrue("cause should be the timeout, chain was $chain", chain.any { it is ServerTimeoutException })
    }

    // --- invoke timeouts ---

    @Test
    fun perCallTimeoutThrowsAndKeepsTheConnection() = runBlocking {
        val connection = connection()
        connection.connect()

        val error = withTimeout(3_000) {
            expect<InvocationTimeoutException> { connection.invoke("Never", timeoutMs = 200) }
        }

        assertEquals(200, error.timeoutMs)
        assertEquals("Never", error.method)
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        assertEquals(JsonPrimitive(7), connection.invoke("Echo", 7, timeoutMs = 2_000))
        connection.disconnect()
    }

    @Test
    fun builderDefaultAppliesAndAPerCallTimeoutOverridesIt() = runBlocking {
        replyDelayMs = 300
        val connection = connection(invocationTimeoutMs = 100)
        connection.connect()

        expect<InvocationTimeoutException> { connection.invoke("Echo", 1) }
        assertEquals(2, connection.invoke<Int>("Echo", 2, timeoutMs = 2_000))
        assertEquals(3, connection.invoke<Int>("Echo", 3, timeoutMs = 0)) // 0 = no limit
        connection.disconnect()
    }

    // --- handlers run off the reader ---

    @Test
    fun aBlockingHandlerDoesNotStallTheConnection() = runBlocking {
        val connection = connection(serverTimeoutMs = 400)
        val handled = CopyOnWriteArrayList<Int>()
        connection.on<Int>("Tick") { n ->
            if (n == 0) Thread.sleep(1_000)
            handled += n
        }
        connection.connect()

        push("Tick", 0)
        delay(50)
        // Results and server frames keep flowing while the handler blocks.
        val started = System.nanoTime()
        repeat(4) {
            assertEquals(it, connection.invoke<Int>("Echo", it))
            delay(150) // keeps the server timeout fed through the echoes
        }
        assertTrue("invokes waited on the handler", (System.nanoTime() - started) / 1_000_000 < 1_000)
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)

        withTimeout(3_000) { while (handled.isEmpty()) delay(10) }
        connection.disconnect()
    }

    @Test
    fun handlersRunOneAtATimeInArrivalOrder() = runBlocking {
        val connection = connection()
        val handled = CopyOnWriteArrayList<Int>()
        val running = AtomicInteger(0)
        val maxRunning = AtomicInteger(0)
        connection.on<Int>("Tick") { n ->
            maxRunning.accumulateAndGet(running.incrementAndGet(), ::maxOf)
            Thread.sleep(2)
            handled += n
            running.decrementAndGet()
        }
        connection.connect()

        repeat(50) { push("Tick", it) }

        withTimeout(5_000) { while (handled.size < 50) delay(10) }
        assertEquals((0 until 50).toList(), handled)
        assertEquals(1, maxRunning.get())
        connection.disconnect()
    }

    @Test
    fun aCancelledSubscriptionSkipsQueuedCalls() = runBlocking {
        val connection = connection()
        val handled = CopyOnWriteArrayList<Int>()
        lateinit var sub: Subscription
        sub = connection.on<Int>("Tick") { n ->
            handled += n
            sub.cancel()
            Thread.sleep(100)
        }
        connection.connect()

        repeat(5) { push("Tick", it) }
        delay(500)

        assertEquals(listOf(0), handled)
        connection.disconnect()
    }

    // --- connect() joins a connect already in progress ---

    @Test
    fun concurrentConnectCallsShareOneAttempt() = runBlocking {
        handshakeDelayMs = 300
        val connection = connection()

        val calls = List(3) { async { connection.connect(); connection.connectionState.value } }

        assertEquals(List(3) { HubConnectionState.CONNECTED }, withTimeout(3_000) { calls.awaitAll() })
        assertEquals(1, socketsOpened.get())
        connection.disconnect()
    }

    @Test
    fun aWaitingConnectGetsTheSameFailure() = runBlocking {
        handshakeDelayMs = 200
        handshakeError = "Protocol not supported"
        val connection = connection()

        val calls = List(2) {
            async { expect<IllegalStateException> { connection.connect() } }
        }

        calls.awaitAll().forEach { assertTrue(it.message, it.message!!.contains("Protocol not supported")) }
        assertEquals(1, socketsOpened.get())
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun aWaitingConnectFailsWhenDisconnectInterrupts() = runBlocking {
        handshakeDelayMs = 500
        val connection = connection()
        val first = async { runCatching { connection.connect() } }
        delay(100)
        val second = async { expect<IllegalStateException> { connection.connect() } }
        delay(100)

        connection.disconnect()

        assertTrue(withTimeout(3_000) { second.await() }.message!!.contains("disconnected or closed"))
        assertTrue(withTimeout(3_000) { first.await() }.isFailure)
    }

    @Test
    fun cancellingTheCallerDoingTheWorkFailsWaitersWithoutCancellingThem() = runBlocking {
        handshakeDelayMs = 500
        val connection = connection()
        val first = launch { connection.connect() }
        delay(100)
        val second = async { expect<IllegalStateException> { connection.connect() } }
        delay(100)

        first.cancel()

        assertTrue(withTimeout(3_000) { second.await() }.message!!.contains("cancelled"))
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun cancellingAWaiterLeavesTheAttemptRunning() = runBlocking {
        handshakeDelayMs = 300
        val connection = connection()
        val first = async { connection.connect() }
        delay(50)
        val waiter = launch { connection.connect() }
        delay(50)

        waiter.cancel()

        withTimeout(3_000) { first.await() }
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }

    @Test
    fun connectDuringReconnectWaitsForIt() = runBlocking {
        val connection = connection(policy = RetryPolicy { 200L })
        connection.connect()

        current!!.close()
        withTimeout(3_000) { connection.connectionState.first { it == HubConnectionState.RECONNECTING } }
        withTimeout(3_000) { connection.connect() }

        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        assertEquals(2, socketsOpened.get())
        connection.disconnect()
    }

    @Test
    fun connectDuringReconnectFailsWhenReconnectGivesUp() = runBlocking {
        val connection = connection(policy = RetryPolicy.fixedDelays(listOf(50, 50)))
        connection.connect()

        handshakeError = "Server is busy"
        current!!.close()
        withTimeout(3_000) { connection.connectionState.first { it == HubConnectionState.RECONNECTING } }

        val error = withTimeout(3_000) { expect<ReconnectFailedException> { connection.connect() } }
        assertEquals(2, error.attempts)
        assertEquals(HubConnectionState.DISCONNECTED, connection.connectionState.value)
    }

    @Test
    fun connectWhenAlreadyConnectedReturnsAtOnce() = runBlocking {
        val connection = connection()
        connection.connect()
        connection.connect()

        assertEquals(1, socketsOpened.get())
        assertEquals(HubConnectionState.CONNECTED, connection.connectionState.value)
        connection.disconnect()
    }
}
