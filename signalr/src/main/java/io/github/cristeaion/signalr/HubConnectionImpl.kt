package io.github.cristeaion.signalr

import io.ktor.client.HttpClient
import io.ktor.client.plugins.websocket.webSocketSession
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.parameter
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.websocket.Frame
import io.ktor.websocket.WebSocketSession
import io.ktor.websocket.close
import io.ktor.websocket.readText
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

private const val TAG = "HubConnection"

internal class HubConnectionImpl(
    private val client: HttpClient,
    private val baseUrl: String,
    private val connectionHeaders: Map<String, String>,
    private val accessTokenProvider: (suspend () -> String?)?,
    private val skipNegotiate: Boolean = false,
    private val logger: SignalRLogger = ConsoleSignalRLogger(),
    private val handshakeTimeoutMs: Long = 15_000L,
    private val keepAliveIntervalMs: Long = HubConnectionBuilder.DEFAULT_KEEP_ALIVE_INTERVAL_MS,
    private val serverTimeoutMs: Long = HubConnectionBuilder.DEFAULT_SERVER_TIMEOUT_MS,
) : HubConnection {

    private val _connectionState = MutableStateFlow(HubConnectionState.DISCONNECTED)
    override val connectionState: StateFlow<HubConnectionState> = _connectionState.asStateFlow()

    private val _errorFlow = MutableSharedFlow<Throwable>(extraBufferCapacity = 64)
    override val errorFlow: SharedFlow<Throwable> = _errorFlow.asSharedFlow()

    private val _eventsFlow = MutableSharedFlow<SignalREvent>(extraBufferCapacity = 128)
    override val eventsFlow: SharedFlow<SignalREvent> = _eventsFlow.asSharedFlow()

    private val isClassic = isClassicSignalRUrl(baseUrl)
    private val classicHubNames = extractClassicHubNames(baseUrl)

    /** Guards the state / session / scope triple so teardown and connect never interleave. */
    private val lock = Any()
    @Volatile private var session: WebSocketSession? = null
    @Volatile private var clientScope: CoroutineScope? = null
    private val closed = AtomicBoolean(false)
    private val invocationIdCounter = AtomicInteger(1)

    /** Monotonic timestamps (System.nanoTime) of the last frame in each direction, for the heartbeat. */
    @Volatile private var lastReceivedNanos = 0L
    @Volatile private var lastSentNanos = 0L

    /** Keyed by lowercase method name - hub method matching is case-insensitive. */
    private val callbacks = ConcurrentHashMap<String, CopyOnWriteArrayList<(List<JsonElement>) -> Unit>>()
    private val pendingInvokes = ConcurrentHashMap<String, CompletableDeferred<JsonElement>>()

    private inline fun log(level: SignalRLogLevel, throwable: Throwable? = null, message: () -> String) =
        logger.log(level, TAG, throwable, message)

    private fun notifyError(throwable: Throwable) {
        _errorFlow.tryEmit(throwable)
    }

    override suspend fun connect() {
        check(!closed.get()) { "HubConnection is closed; build a new one to connect again." }
        val scope = synchronized(lock) {
            if (_connectionState.value != HubConnectionState.DISCONNECTED) {
                log(SignalRLogLevel.INFO) { "Connect skipped: HubConnection is already ${_connectionState.value}." }
                return
            }
            _connectionState.value = HubConnectionState.CONNECTING
            CoroutineScope(Dispatchers.IO + SupervisorJob()).also { clientScope = it }
        }
        log(SignalRLogLevel.INFO) { "Initiating connection to ${withoutQuery(baseUrl)} (skipNegotiate=$skipNegotiate)..." }

        try {
            establish(scope)
        } catch (t: Throwable) {
            if (t !is CancellationException && t !is ConnectAbortedException) {
                log(SignalRLogLevel.ERROR, t) { "Connecting to ${withoutQuery(baseUrl)} failed" }
                notifyError(t)
            }
            // Whatever got opened before the failure - the WebSocket, the scope - is released here.
            detach(scope)?.let { release(it) }
            throw t
        }
    }

    private suspend fun establish(scope: CoroutineScope) {
        val token = accessTokenProvider?.invoke()
        val headersWithAuth = connectionHeaders.toMutableMap()
        if (!headersWithAuth.containsKey("User-Agent")) {
            headersWithAuth["User-Agent"] = "Mozilla/5.0 (Android; EasyMarkets SignalR)"
        }
        if (token != null) {
            headersWithAuth["Authorization"] = "Bearer $token"
            log(SignalRLogLevel.VERBOSE) { "Authorization token injected." }
        }

        var negotiateResponse: NegotiateResponse? = null
        if (!skipNegotiate) {
            negotiateResponse = negotiate(baseUrl, headersWithAuth)
            if (negotiateResponse.error != null) {
                throw RuntimeException("Negotiate error: ${negotiateResponse.error}")
            }
            log(SignalRLogLevel.INFO) { "Negotiation successful." }
            // Secrets and payloads (tokens, URLs with queries, hub arguments, server data) are
            // logged at VERBOSE only.
            log(SignalRLogLevel.VERBOSE) { "Negotiated connection token: ${negotiateResponse.getEffectiveConnectionToken()}" }
        } else {
            log(SignalRLogLevel.INFO) { "Skipping negotiation phase as requested." }
        }

        val wsUrl = buildWebSocketUrl(baseUrl, negotiateResponse, token)
        log(SignalRLogLevel.INFO) { "Opening WebSocket connection..." }
        log(SignalRLogLevel.VERBOSE) { "WebSocket URL: $wsUrl" }

        val ws = client.webSocketSession(urlString = wsUrl) {
            headersWithAuth.forEach { entry -> header(entry.key, entry.value) }
        }
        // Publish the session at once so a failure below still closes it.
        val ownsScope = synchronized(lock) {
            (clientScope === scope).also { if (it) session = ws }
        }
        if (!ownsScope) {
            ws.closeQuietly()
            throw ConnectAbortedException()
        }

        if (isClassic) {
            // Classic servers cannot deserialize the Core handshake frame, so it must not be
            // sent. They instead push an init frame ({"S":1,...}) and then wait for /start.
            val initFrame = withTimeoutOrNull(handshakeTimeoutMs.milliseconds) {
                ws.incoming.receive() as? Frame.Text
            }
            if (initFrame != null) {
                val frameText = initFrame.readText()
                log(SignalRLogLevel.DEBUG) { "Received classic init frame." }
                log(SignalRLogLevel.VERBOSE) { "Classic init frame: $frameText" }
                handleIncomingPayload(frameText)
            } else {
                log(SignalRLogLevel.WARN) { "No classic init frame arrived within ${handshakeTimeoutMs}ms; continuing to /start anyway." }
            }
            startClassicConnection(negotiateResponse, headersWithAuth)
        } else {
            performHandshake(ws)
        }

        synchronized(lock) {
            if (clientScope !== scope || closed.get()) {
                throw ConnectAbortedException()
            }
            val now = System.nanoTime()
            lastReceivedNanos = now
            lastSentNanos = now
            _connectionState.value = HubConnectionState.CONNECTED
        }
        log(SignalRLogLevel.INFO) { "WebSocket connected successfully! Connection state is now CONNECTED." }

        startReader(scope, ws)
        startHeartbeat(scope, ws, effectiveServerTimeoutMs(negotiateResponse))
    }

    private suspend fun performHandshake(ws: WebSocketSession) {
        log(SignalRLogLevel.DEBUG) { "Sending SignalR handshake request frame..." }
        sendText(ws, SignalRProtocol.createHandshakeMessage())
        val firstFrame = withTimeoutOrNull(handshakeTimeoutMs.milliseconds) { ws.incoming.receive() }
            ?: throw IllegalStateException("Handshake failed: no response from the server within ${handshakeTimeoutMs}ms.")
        if (firstFrame !is Frame.Text) {
            throw IllegalStateException("Handshake failed: expected a text frame, got ${firstFrame.frameType}.")
        }
        val frameText = firstFrame.readText()
        log(SignalRLogLevel.DEBUG) { "Received handshake response frame." }
        log(SignalRLogLevel.VERBOSE) { "Handshake response frame: $frameText" }
        val frames = SignalRProtocol.parseFrames(frameText)
        val handshakeResponse = frames.firstOrNull()
            ?.let { SignalRProtocol.json.decodeFromString<HandshakeResponse>(it) }
            ?: throw IllegalStateException("Handshake failed: empty response.")
        if (handshakeResponse.error != null) {
            throw IllegalStateException("Handshake failed: ${handshakeResponse.error}")
        }
        log(SignalRLogLevel.DEBUG) { "ASP.NET Core SignalR handshake completed successfully." }
        // The server may batch its first messages into the handshake frame.
        if (frames.size > 1) {
            handleIncomingPayload(frames.drop(1).joinToString("") { it + SignalRProtocol.RECORD_SEPARATOR })
        }
    }

    /**
     * Core: the configured server timeout. Classic: the server dictates it through
     * `KeepAliveTimeout` in negotiate; without one, keep-alive is off and nothing is enforced.
     */
    private fun effectiveServerTimeoutMs(negotiateResponse: NegotiateResponse?): Long =
        if (isClassic) {
            negotiateResponse?.keepAliveTimeout?.takeIf { it > 0 }?.let { (it * 1000).toLong() } ?: 0L
        } else {
            serverTimeoutMs
        }

    private fun startReader(scope: CoroutineScope, ws: WebSocketSession) {
        scope.launch {
            log(SignalRLogLevel.DEBUG) { "Started background WebSocket frame reader loop." }
            try {
                for (frame in ws.incoming) {
                    lastReceivedNanos = System.nanoTime()
                    if (frame is Frame.Text) {
                        handleIncomingPayload(frame.readText())
                    }
                }
            } catch (ce: CancellationException) {
                log(SignalRLogLevel.INFO) { "WebSocket frame reader cancelled gracefully." }
            } catch (t: Throwable) {
                log(SignalRLogLevel.ERROR, t) { "WebSocket frame reader error" }
                notifyError(t)
            } finally {
                log(SignalRLogLevel.INFO) { "WebSocket frame reader loop terminated." }
                detach(scope)?.let { release(it) }
            }
        }
    }

    /**
     * Two loops on the connection's scope:
     * - keep-alive (Core only): sends a ping whenever nothing has been sent for [keepAliveIntervalMs];
     * - server timeout: drops the connection once nothing has arrived for [timeoutMs].
     */
    private fun startHeartbeat(scope: CoroutineScope, ws: WebSocketSession, timeoutMs: Long) {
        if (!isClassic && keepAliveIntervalMs > 0) {
            scope.launch {
                while (isActive) {
                    val idleMs = elapsedMs(lastSentNanos)
                    if (idleMs < keepAliveIntervalMs) {
                        delay(keepAliveIntervalMs - idleMs)
                        continue
                    }
                    try {
                        sendText(ws, SignalRProtocol.PING_MESSAGE)
                        log(SignalRLogLevel.VERBOSE) { "Sent keep-alive ping." }
                    } catch (ce: CancellationException) {
                        throw ce
                    } catch (t: Throwable) {
                        // The socket is going away; the reader loop sees it and tears down.
                        log(SignalRLogLevel.WARN, t) { "Keep-alive ping failed; stopping keep-alive." }
                        return@launch
                    }
                }
            }
        }

        if (timeoutMs > 0) {
            log(SignalRLogLevel.DEBUG) { "Server timeout armed: ${timeoutMs}ms." }
            scope.launch {
                while (isActive) {
                    val remainingMs = timeoutMs - elapsedMs(lastReceivedNanos)
                    if (remainingMs > 0) {
                        delay(remainingMs)
                        continue
                    }
                    val error = ServerTimeoutException(timeoutMs)
                    log(SignalRLogLevel.WARN, error) { "Server went silent; closing the connection." }
                    val detached = detach(scope) ?: return@launch
                    notifyError(error)
                    release(detached)
                    return@launch
                }
            }
        }
    }

    private fun elapsedMs(sinceNanos: Long): Long = (System.nanoTime() - sinceNanos) / 1_000_000

    private suspend fun sendText(ws: WebSocketSession, text: String) {
        ws.send(Frame.Text(text))
        lastSentNanos = System.nanoTime()
    }

    private suspend fun startClassicConnection(
        negotiateResponse: NegotiateResponse?,
        headers: Map<String, String>,
    ) {
        val startUrl = getStartUrl(baseUrl, negotiateResponse?.getEffectiveConnectionToken())
        log(SignalRLogLevel.DEBUG) { "Sending classic start request..." }
        log(SignalRLogLevel.VERBOSE) { "Classic start URL: $startUrl" }
        val response = client.get(startUrl) {
            headers.forEach { entry -> header(entry.key, entry.value) }
        }

        val body = response.bodyAsText()
        if (response.status.value !in 200..299) {
            throw RuntimeException("Classic start failed with status ${response.status}: $body")
        }
        log(SignalRLogLevel.INFO) { "Classic start acknowledged." }
        log(SignalRLogLevel.VERBOSE) { "Classic start response: $body" }
    }

    private suspend fun negotiate(baseUrl: String, headers: Map<String, String>): NegotiateResponse {
        val negotiateUrl = getNegotiateUrl(baseUrl)
        log(SignalRLogLevel.DEBUG) { "Sending negotiate POST request to ${withoutQuery(negotiateUrl)}" }

        val response = client.post(negotiateUrl) {
            headers.forEach { entry -> header(entry.key, entry.value) }
            parameter("negotiateVersion", "1")
            parameter("clientProtocol", "1.5")
        }

        log(SignalRLogLevel.DEBUG) { "Negotiate response HTTP status: ${response.status}" }

        if (response.status.value !in 200..299) {
            throw RuntimeException("Negotiate failed with status ${response.status}")
        }

        val responseText = response.bodyAsText()
        log(SignalRLogLevel.VERBOSE) { "Raw Negotiate response payload: $responseText" }
        return SignalRProtocol.json.decodeFromString<NegotiateResponse>(responseText)
    }

    private fun dispatchCallbacks(target: String, args: List<JsonElement>) {
        _eventsFlow.tryEmit(SignalREvent(target, args))

        val handlers = callbacks[target.lowercase()] ?: return
        log(SignalRLogLevel.INFO) { "Dispatching '$target' to ${handlers.size} handler(s)" }
        handlers.forEach { handler ->
            try {
                handler(args)
            } catch (t: Throwable) {
                log(SignalRLogLevel.ERROR, t) { "Handler for '$target' threw exception" }
                notifyError(t)
            }
        }
    }

    /**
     * Routes a server result to whoever called [invoke]. With no pending call - a [send] that
     * carried its own id, or a late reply - the outcome is logged or surfaced on [errorFlow]
     * rather than dropped.
     */
    private fun completeInvocation(id: String, result: JsonElement?, error: String?) {
        val deferred = pendingInvokes.remove(id)
        if (error != null) {
            val err = RuntimeException("Invocation '$id' failed: $error")
            if (deferred != null) deferred.completeExceptionally(err) else notifyError(err)
        } else {
            val value = result ?: JsonNull
            if (deferred != null) {
                deferred.complete(value)
            } else {
                log(SignalRLogLevel.INFO) { "Result for '$id' arrived with nothing awaiting it." }
                log(SignalRLogLevel.VERBOSE) { "Unawaited result for '$id': $value" }
            }
        }
    }

    private fun handleIncomingPayload(payload: String) {
        log(SignalRLogLevel.VERBOSE) { "Handling incoming payload (${payload.length} chars): $payload" }
        val frames = SignalRProtocol.parseFrames(payload)
        for (frameText in frames) {
            try {
                val msg = SignalRProtocol.json.decodeFromString<HubMessageJson>(frameText)
                if (msg.type != 0 || msg.target != null) {
                    log(SignalRLogLevel.DEBUG) { "Parsed HubMessageJson (type=${msg.type}, target=${msg.target}, invocationId=${msg.invocationId})" }
                    when (msg.type) {
                        1 -> { // Invocation
                            val target = msg.target ?: continue
                            val args = msg.arguments ?: emptyList()
                            dispatchCallbacks(target, args)
                        }

                        3 -> { // Completion
                            val id = msg.invocationId ?: continue
                            log(SignalRLogLevel.DEBUG) { "Received completion for invocationId '$id'. Error=${msg.error}" }
                            completeInvocation(id, msg.result, msg.error)
                        }

                        // Ping: receiving it already reset the server timeout; it needs no reply.
                        6 -> log(SignalRLogLevel.VERBOSE) { "Received keep-alive ping." }
                    }
                    continue
                }
            } catch (_: Throwable) {
                // Fall back to Classic ASP.NET SignalR frame parsing
            }

            try {
                val classicFrame = SignalRProtocol.json.decodeFromString<ClassicHubFrame>(frameText)
                log(SignalRLogLevel.DEBUG) { "Parsed ClassicHubFrame with ${classicFrame.M?.size ?: 0} invocation(s)." }

                // Classic servers answer an invocation with {"I":"<id>","R":<result>} / {"I":..,"E":..}
                // rather than a Core type-3 completion message.
                val completionId = classicFrame.I
                if (completionId != null) {
                    log(SignalRLogLevel.DEBUG) { "Received classic completion for invocationId '$completionId'. Error=${classicFrame.E}" }
                    completeInvocation(completionId, classicFrame.R, classicFrame.E)
                }

                classicFrame.M?.forEach { invocation ->
                    val method = invocation.M
                    val hub = invocation.H
                    val args = invocation.A ?: emptyList()

                    if (method != null) {
                        dispatchCallbacks(method, args)
                        if (hub != null) {
                            dispatchCallbacks("$hub.$method", args)
                        }
                    } else if (hub != null) {
                        dispatchCallbacks(hub, args)
                    }
                }
            } catch (_: Throwable) {
                log(SignalRLogLevel.WARN) { "Unrecognized frame (${frameText.length} chars) ignored." }
                log(SignalRLogLevel.VERBOSE) { "Unrecognized frame payload: $frameText" }
            }
        }
    }

    override suspend fun disconnect() {
        log(SignalRLogLevel.INFO) { "Disconnect requested by caller." }
        detach()?.let { release(it) }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        log(SignalRLogLevel.INFO) { "Close requested: disconnecting and releasing the HTTP client." }
        val detached = detach()
        // close() must not block, so the graceful WebSocket close runs in the background; the
        // client is closed only after it, or the close frame would never be written.
        CoroutineScope(Dispatchers.IO).launch {
            try {
                detached?.let { release(it) }
            } finally {
                client.close()
                log(SignalRLogLevel.INFO) { "HTTP client released." }
            }
        }
    }

    /** [disconnect] or [close] ran while [connect] was in flight. Thrown to the connect caller only. */
    private class ConnectAbortedException :
        IllegalStateException("Connection was disconnected or closed while connecting.")

    private class Detached(val session: WebSocketSession?, val scope: CoroutineScope?)

    /**
     * Synchronously moves to DISCONNECTED and hands back what must be released. With [owner]
     * set, only the connection that started from that scope is torn down, so a stale reader or
     * timer can never kill a newer connection. Returns null when there was nothing to tear down.
     */
    private fun detach(owner: CoroutineScope? = null): Detached? {
        val detached = synchronized(lock) {
            if (owner != null && clientScope !== owner) return null
            if (_connectionState.value == HubConnectionState.DISCONNECTED) return null
            _connectionState.value = HubConnectionState.DISCONNECTED
            Detached(session, clientScope).also {
                session = null
                clientScope = null
            }
        }
        pendingInvokes.values.forEach { it.cancel() }
        pendingInvokes.clear()
        log(SignalRLogLevel.INFO) { "HubConnection is now DISCONNECTED." }
        return detached
    }

    /** Closes the WebSocket gracefully, then cancels the connection's coroutines. */
    private suspend fun release(detached: Detached) {
        log(SignalRLogLevel.DEBUG) { "Closing WebSocket session and cancelling client coroutine scope..." }
        // NonCancellable: the caller may itself be one of the coroutines being torn down.
        withContext(NonCancellable) { detached.session?.closeQuietly() }
        detached.scope?.cancel()
    }

    private suspend fun WebSocketSession.closeQuietly() {
        try {
            close()
        } catch (t: Throwable) {
            log(SignalRLogLevel.VERBOSE) { "Error closing WebSocket session: ${t.message}" }
        }
    }

    private fun formatOutgoingMessage(method: String, args: Array<out Any>, invocationId: String?): String {
        // A caller may hand us a ready-made classic envelope, either as the method itself
        // (e.g. {"H":"quoteshub","M":"Subscribe..."}) or as a single string argument.
        SignalRProtocol.asClassicEnvelope(method)?.let {
            return SignalRProtocol.renderClassicEnvelope(it, invocationId)
        }
        if (args.size == 1) {
            SignalRProtocol.asClassicEnvelope(args[0])?.let {
                return SignalRProtocol.renderClassicEnvelope(it, invocationId)
            }
        }

        if (isClassic) {
            // "quoteshub.GetSnapshot" names the hub inline; a bare name only works when the
            // connection declares exactly one hub.
            val separator = method.lastIndexOf('.')
            val hub = if (separator > 0) method.substring(0, separator) else classicHubNames.singleOrNull()
            val target = if (separator > 0) method.substring(separator + 1) else method
            if (hub == null) {
                throw IllegalArgumentException(
                    "Classic SignalR connection declares ${classicHubNames.size} hubs $classicHubNames, " +
                        "so '$method' is ambiguous. Qualify it as '<hub>.$method', or pass the full " +
                        "envelope {\"H\":\"<hub>\",\"M\":\"$method\",\"A\":[...]}"
                )
            }
            return SignalRProtocol.createClassicInvocationMessage(hub, target, invocationId, args)
        }

        return SignalRProtocol.createInvocationMessage(method, invocationId, args)
    }

    override suspend fun send(method: String, vararg args: Any) {
        val activeSession = requireConnected(method)
        val frameText = formatOutgoingMessage(method, args, null)
        log(SignalRLogLevel.DEBUG) { "Sending '${describeMethod(method)}'." }
        log(SignalRLogLevel.VERBOSE) { "Outgoing frame: $frameText" }
        sendText(activeSession, frameText)
    }

    override suspend fun invoke(method: String, vararg args: Any): JsonElement {
        val activeSession = requireConnected(method)
        val id = invocationIdCounter.getAndIncrement().toString()
        val deferred = CompletableDeferred<JsonElement>()
        pendingInvokes[id] = deferred

        return try {
            val frameText = formatOutgoingMessage(method, args, id)
            log(SignalRLogLevel.DEBUG) { "Invoking '${describeMethod(method)}' (id=$id)." }
            log(SignalRLogLevel.VERBOSE) { "Outgoing invocation frame (id=$id): $frameText" }
            sendText(activeSession, frameText)
            deferred.await()
        } catch (t: Throwable) {
            log(SignalRLogLevel.ERROR, t) { "Invocation '${describeMethod(method)}' (id=$id) failed" }
            throw t
        } finally {
            pendingInvokes.remove(id)
        }
    }

    /**
     * The method name for logs and error messages. A caller may pass a whole classic envelope
     * as [method]; only its `hub.method` is shown, never its arguments.
     */
    private fun describeMethod(method: String): String {
        val envelope = SignalRProtocol.asClassicEnvelope(method) ?: return method
        val hub = (envelope["H"] as? JsonPrimitive)?.contentOrNull
        val name = (envelope["M"] as? JsonPrimitive)?.contentOrNull
        return listOfNotNull(hub, name).joinToString(".").ifEmpty { "<classic envelope>" }
    }

    private fun requireConnected(method: String): WebSocketSession {
        val activeSession = session
        if (_connectionState.value != HubConnectionState.CONNECTED || activeSession == null) {
            throw IllegalStateException("Cannot call '${describeMethod(method)}': connection is ${_connectionState.value}.")
        }
        return activeSession
    }

    override fun on(method: String, handler: (List<JsonElement>) -> Unit): Subscription {
        log(SignalRLogLevel.DEBUG) { "Registered handler for '$method'" }
        val handlers = callbacks.getOrPut(method.lowercase()) { CopyOnWriteArrayList() }
        handlers.add(handler)
        return Subscription { handlers.remove(handler) }
    }
}
