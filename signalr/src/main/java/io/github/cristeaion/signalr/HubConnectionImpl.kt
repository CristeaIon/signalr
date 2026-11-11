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
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
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

/** Sent unless the app sets its own `User-Agent` header. */
private const val CLASSIC_ABORT_TIMEOUT_MS = 2_000L

/** Same limit as the official clients. */
private const val MAX_NEGOTIATE_REDIRECTS = 100

internal const val DEFAULT_USER_AGENT = "signalr-kotlin/${BuildConfig.LIBRARY_VERSION}"

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
    /** Null: a dropped connection stays DISCONNECTED. Otherwise it reconnects as this policy says. */
    private val retryPolicy: RetryPolicy? = null,
    /** Default for [invoke] calls that don't pass their own timeout; 0 = wait as long as the connection lasts. */
    private val invocationTimeoutMs: Long = 0,
) : HubConnection {

    private val _connectionState = MutableStateFlow(HubConnectionState.DISCONNECTED)
    override val connectionState: StateFlow<HubConnectionState> = _connectionState.asStateFlow()

    private val _errorFlow = MutableSharedFlow<Throwable>(extraBufferCapacity = 64)
    override val errorFlow: SharedFlow<Throwable> = _errorFlow.asSharedFlow()

    private val _eventsFlow = MutableSharedFlow<SignalREvent>(extraBufferCapacity = 128)
    override val eventsFlow: SharedFlow<SignalREvent> = _eventsFlow.asSharedFlow()

    private val _reconnectedFlow = MutableSharedFlow<Unit>(extraBufferCapacity = 8)
    override val reconnectedFlow: SharedFlow<Unit> = _reconnectedFlow.asSharedFlow()

    private val isClassic = isClassicSignalRUrl(baseUrl)
    private val classicHubNames = extractClassicHubNames(baseUrl)

    /** Guards state, session, scope and reconnect job so teardown and connect never interleave. */
    private val lock = Any()
    @Volatile private var session: WebSocketSession? = null
    @Volatile private var clientScope: CoroutineScope? = null
    /** Classic only: how to tell the server the connection is over. Set once the connection is CONNECTED. */
    @Volatile private var classicAbort: ClassicAbort? = null
    private class ClassicAbort(val url: String, val headers: Map<String, String>)

    /** The running reconnect loop, set only while the state is RECONNECTING. */
    @Volatile private var reconnectJob: Job? = null
    /**
     * The outcome of the connect or reconnect in progress, set while the state is CONNECTING or
     * RECONNECTING. Completed when the state reaches CONNECTED, failed when it falls back to
     * DISCONNECTED; a [connect] call that arrives meanwhile waits on it.
     */
    private var inFlight: CompletableDeferred<Unit>? = null
    /** Hosts reconnect loops: they must outlive the per-connection scope that just died. Cancelled by [close]. */
    private val reconnectScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val closed = AtomicBoolean(false)
    private val invocationIdCounter = AtomicInteger(1)

    /** Monotonic timestamps (System.nanoTime) of the last frame in each direction, for the heartbeat. */
    @Volatile private var lastReceivedNanos = 0L
    @Volatile private var lastSentNanos = 0L

    /** Keyed by lowercase method name - hub method matching is case-insensitive. */
    private val callbacks = ConcurrentHashMap<String, CopyOnWriteArrayList<(List<JsonElement>) -> Unit>>()
    private val pendingInvokes = ConcurrentHashMap<String, CompletableDeferred<JsonElement>>()

    /**
     * Handlers run here, one call at a time and in arrival order, so a slow handler never stalls
     * the reader: frames keep flowing, invoke results and pings keep arriving. Lives until [close].
     */
    private class HandlerCall(val target: String, val args: List<JsonElement>)
    private val handlerQueue = Channel<HandlerCall>(Channel.UNLIMITED)
    private val handlerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    init {
        handlerScope.launch {
            for (call in handlerQueue) runHandlers(call)
        }
    }

    private inline fun log(level: SignalRLogLevel, throwable: Throwable? = null, message: () -> String) =
        logger.log(level, TAG, throwable, message)

    private fun notifyError(throwable: Throwable) {
        _errorFlow.tryEmit(throwable)
    }

    override suspend fun connect() {
        check(!closed.get()) { "HubConnection is closed; build a new one to connect again." }
        var inProgress: CompletableDeferred<Unit>? = null
        val scope = synchronized(lock) {
            when (_connectionState.value) {
                HubConnectionState.CONNECTED -> {
                    log(SignalRLogLevel.INFO) { "Connect skipped: HubConnection is already CONNECTED." }
                    return
                }
                HubConnectionState.CONNECTING, HubConnectionState.RECONNECTING -> {
                    inProgress = inFlight
                    null
                }
                HubConnectionState.DISCONNECTED -> {
                    _connectionState.value = HubConnectionState.CONNECTING
                    inFlight = CompletableDeferred()
                    newConnectionScope().also { clientScope = it }
                }
            }
        }
        if (scope == null) {
            // Join the connect or reconnect already in progress: return once it connects, or
            // throw why it didn't. Cancelling this call leaves that attempt running.
            log(SignalRLogLevel.INFO) { "Connect is already in progress; waiting for it." }
            inProgress?.await()
            return
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
            // Callers waiting on this attempt get the same failure, but never a CancellationException:
            // only this caller was cancelled, not them.
            val forWaiters = if (t is CancellationException) {
                ConnectAbortedException("The connect() call in progress was cancelled.")
            } else {
                t
            }
            detach(scope, inFlightFailure = forWaiters)?.let { release(it) }
            throw t
        }
    }

    private fun newConnectionScope() = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * Negotiates, opens the WebSocket, handshakes and moves to CONNECTED. Used by [connect] and by
     * each reconnect attempt; the caller owns the state before it and cleans up if it throws.
     */
    private suspend fun establish(scope: CoroutineScope) {
        val token = accessTokenProvider?.invoke()
        val headersWithAuth = connectionHeaders.toMutableMap()
        if (headersWithAuth.keys.none { it.equals("User-Agent", ignoreCase = true) }) {
            headersWithAuth["User-Agent"] = DEFAULT_USER_AGENT
        }
        if (token != null) {
            headersWithAuth["Authorization"] = "Bearer $token"
            log(SignalRLogLevel.VERBOSE) { "Authorization token injected." }
        }

        var negotiateResponse: NegotiateResponse? = null
        // A Core server may redirect negotiate (Azure SignalR Service does), handing out the URL
        // and token to use from then on.
        var hubUrl = baseUrl
        var hubToken = token
        if (!skipNegotiate) {
            var redirects = 0
            while (true) {
                val response = negotiate(hubUrl, headersWithAuth)
                if (response.error != null) {
                    throw NegotiateException("Negotiate error: ${response.error}")
                }
                val redirectUrl = response.url
                if (isClassic || redirectUrl.isNullOrEmpty()) {
                    negotiateResponse = response
                    break
                }
                if (++redirects > MAX_NEGOTIATE_REDIRECTS) {
                    throw NegotiateException("Negotiate redirected more than $MAX_NEGOTIATE_REDIRECTS times.")
                }
                log(SignalRLogLevel.INFO) { "Negotiate redirected to ${withoutQuery(redirectUrl)}." }
                hubUrl = redirectUrl
                response.accessToken?.let { redirectToken ->
                    hubToken = redirectToken
                    headersWithAuth.keys.removeAll { it.equals("Authorization", ignoreCase = true) }
                    headersWithAuth["Authorization"] = "Bearer $redirectToken"
                }
            }
            requireWebSockets(negotiateResponse)
            log(SignalRLogLevel.INFO) { "Negotiation successful." }
            // Secrets and payloads (tokens, URLs with queries, hub arguments, server data) are
            // logged at VERBOSE only.
            log(SignalRLogLevel.VERBOSE) { "Negotiated connection token: ${negotiateResponse.getEffectiveConnectionToken()}" }
        } else {
            log(SignalRLogLevel.INFO) { "Skipping negotiation phase as requested." }
        }

        val wsUrl = buildWebSocketUrl(hubUrl, negotiateResponse, hubToken)
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
            if (isClassic) {
                classicAbort = ClassicAbort(
                    getAbortUrl(baseUrl, negotiateResponse?.getEffectiveConnectionToken()),
                    headersWithAuth,
                )
            }
            inFlight?.complete(Unit)
            inFlight = null
        }
        log(SignalRLogLevel.INFO) { "WebSocket connected successfully! Connection state is now CONNECTED." }

        startReader(scope, ws)
        startHeartbeat(scope, ws, effectiveServerTimeoutMs(negotiateResponse))
    }

    /** This client only speaks WebSockets with text frames; fail clearly if the server won't. */
    private fun requireWebSockets(response: NegotiateResponse) {
        if (isClassic) {
            if (response.tryWebSockets == false) {
                throw NegotiateException("The server doesn't allow WebSockets (TryWebSockets=false), the only transport this client supports.")
            }
            return
        }
        val transports = response.availableTransports ?: return
        val webSockets = transports.firstOrNull { it.transport.equals("WebSockets", ignoreCase = true) }
            ?: throw NegotiateException(
                "The server doesn't offer WebSockets, the only transport this client supports " +
                    "(it offers ${transports.map { it.transport }}).",
            )
        if (webSockets.transferFormats.isNotEmpty() && webSockets.transferFormats.none { it.equals("Text", ignoreCase = true) }) {
            throw NegotiateException("The server's WebSockets transport doesn't support text frames, which the JSON protocol needs.")
        }
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
            val close = handleIncomingPayload(frames.drop(1).joinToString("") { it + SignalRProtocol.RECORD_SEPARATOR })
            if (close != null) {
                throw IllegalStateException("Server closed the connection during the handshake: ${close.error ?: "no error given"}")
            }
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
            var cause: Throwable? = null
            var allowReconnect = true
            try {
                for (frame in ws.incoming) {
                    lastReceivedNanos = System.nanoTime()
                    if (frame !is Frame.Text) continue
                    val close = handleIncomingPayload(frame.readText()) ?: continue
                    allowReconnect = close.allowReconnect
                    if (close.error != null) {
                        cause = HubException("Server closed the connection: ${close.error}", close.error).also { notifyError(it) }
                    }
                    break
                }
            } catch (ce: CancellationException) {
                log(SignalRLogLevel.INFO) { "WebSocket frame reader cancelled gracefully." }
            } catch (t: Throwable) {
                log(SignalRLogLevel.ERROR, t) { "WebSocket frame reader error" }
                notifyError(t)
                cause = t
            } finally {
                log(SignalRLogLevel.INFO) { "WebSocket frame reader loop terminated." }
                onConnectionLost(scope, cause, allowReconnect)
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
                    if (clientScope !== scope) return@launch
                    val error = ServerTimeoutException(timeoutMs)
                    log(SignalRLogLevel.WARN, error) { "Server went silent; closing the connection." }
                    notifyError(error)
                    onConnectionLost(scope, error, allowReconnect = true)
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
            // The body stays out of the message: exceptions are logged at ERROR, payloads only at VERBOSE.
            log(SignalRLogLevel.VERBOSE) { "Classic start error response: $body" }
            throw SignalRHttpException("Classic start failed with status ${response.status}.", response.status.value)
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
            throw NegotiateException("Negotiate failed with status ${response.status}.", response.status.value)
        }

        val responseText = response.bodyAsText()
        log(SignalRLogLevel.VERBOSE) { "Raw Negotiate response payload: $responseText" }
        return SignalRProtocol.json.decodeFromString<NegotiateResponse>(responseText)
    }

    private fun dispatchCallbacks(target: String, args: List<JsonElement>) {
        _eventsFlow.tryEmit(SignalREvent(target, args))
        if (callbacks[target.lowercase()].isNullOrEmpty()) return
        handlerQueue.trySend(HandlerCall(target, args))
    }

    /** Runs on [handlerScope]. Handlers are looked up now, so one removed while queued doesn't run. */
    private fun runHandlers(call: HandlerCall) {
        val target = call.target
        val args = call.args
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
            val err = HubException("Invocation '$id' failed: $error", error)
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

    /** A Core close message (type 7). */
    private class ServerClose(val error: String?, val allowReconnect: Boolean)

    /**
     * Dispatches every frame in [payload]. Returns the server's close message if there is one;
     * frames after it are ignored, and the caller ends the connection.
     */
    private fun handleIncomingPayload(payload: String): ServerClose? {
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

                        7 -> { // Close
                            // Like the official clients: no reconnect unless the server allows it.
                            val allowReconnect = msg.allowReconnect == true
                            log(SignalRLogLevel.INFO) { "Server sent a close message (allowReconnect=$allowReconnect, error=${msg.error != null})." }
                            log(SignalRLogLevel.VERBOSE) { "Close message error: ${msg.error}" }
                            return ServerClose(msg.error, allowReconnect)
                        }
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

                val command = when {
                    classicFrame.D == 1 -> ServerClose(error = null, allowReconnect = false)
                    classicFrame.T == 1 -> ServerClose(error = null, allowReconnect = true)
                    else -> null
                }
                if (command != null) {
                    log(SignalRLogLevel.INFO) { "Classic server asked to ${if (command.allowReconnect) "reconnect" else "disconnect"}." }
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
                if (command != null) return command
            } catch (_: Throwable) {
                log(SignalRLogLevel.WARN) { "Unrecognized frame (${frameText.length} chars) ignored." }
                log(SignalRLogLevel.VERBOSE) { "Unrecognized frame payload: $frameText" }
            }
        }
        return null
    }

    override suspend fun disconnect() {
        log(SignalRLogLevel.INFO) { "Disconnect requested by caller." }
        detach()?.let { release(it) }
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        log(SignalRLogLevel.INFO) { "Close requested: disconnecting and releasing the HTTP client." }
        val detached = detach()
        reconnectScope.cancel()
        handlerQueue.close()
        handlerScope.cancel()
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

    /**
     * [disconnect] or [close] ran while a connect was in flight, or (for callers waiting on it)
     * the connect() call doing the work was cancelled.
     */
    private class ConnectAbortedException(
        message: String = "Connection was disconnected or closed while connecting.",
    ) : IllegalStateException(message)

    private class Detached(
        val session: WebSocketSession?,
        val scope: CoroutineScope?,
        val reconnectJob: Job? = null,
        val classicAbort: ClassicAbort? = null,
    )

    /**
     * Synchronously moves to DISCONNECTED, fails waiting [connect] and [invoke] calls (with
     * [inFlightFailure], and a [ConnectionLostException] carrying [lostCause]), and hands back
     * what must be released. With [owner]
     * set, only the connection that started from that scope is torn down, so a stale reader or
     * timer can never kill a newer connection. Returns null when there was nothing to tear down.
     */
    private fun detach(
        owner: CoroutineScope? = null,
        inFlightFailure: Throwable? = null,
        lostCause: Throwable? = null,
        lost: Boolean = false,
    ): Detached? {
        val detached = synchronized(lock) {
            if (owner != null && clientScope !== owner) return null
            if (_connectionState.value == HubConnectionState.DISCONNECTED) return null
            _connectionState.value = HubConnectionState.DISCONNECTED
            inFlight?.completeExceptionally(inFlightFailure ?: ConnectAbortedException())
            inFlight = null
            Detached(session, clientScope, reconnectJob, classicAbort).also {
                session = null
                clientScope = null
                reconnectJob = null
                classicAbort = null
            }
        }
        failPendingInvokes(if (lost) "Connection was lost" else "Connection was closed", lostCause)
        log(SignalRLogLevel.INFO) { "HubConnection is now DISCONNECTED." }
        return detached
    }

    /** Fails every [invoke] still waiting for a result with a [ConnectionLostException]. */
    private fun failPendingInvokes(reason: String, cause: Throwable?) {
        val ids = pendingInvokes.keys.toList()
        ids.forEach { id ->
            pendingInvokes.remove(id)?.completeExceptionally(
                ConnectionLostException("$reason before the result of invocation '$id' arrived.", cause),
            )
        }
    }

    /** Closes the WebSocket gracefully, then stops the connection's coroutines and any reconnect loop. */
    private suspend fun release(detached: Detached) {
        log(SignalRLogLevel.DEBUG) { "Closing WebSocket session and cancelling client coroutine scope..." }
        detached.reconnectJob?.cancel()
        // NonCancellable: the caller may itself be one of the coroutines being torn down.
        withContext(NonCancellable) {
            detached.session?.closeQuietly()
            detached.classicAbort?.let { sendClassicAbort(it) }
        }
        detached.scope?.cancel()
    }

    /**
     * The connection owned by [scope] dropped without the caller asking for it: the socket failed
     * or closed, the server timed out, or the server sent a close message. Reconnects when a
     * [retryPolicy] is set and the server allows it; otherwise moves to DISCONNECTED. Does nothing
     * if [scope] no longer owns the connection (it was already disconnected or replaced).
     */
    private suspend fun onConnectionLost(scope: CoroutineScope, cause: Throwable?, allowReconnect: Boolean) {
        val policy = retryPolicy
        if (policy == null || !allowReconnect) {
            detach(scope, lostCause = cause, lost = true)?.let { release(it) }
            return
        }
        val detached = synchronized(lock) {
            if (clientScope !== scope || closed.get()) return
            _connectionState.value = HubConnectionState.RECONNECTING
            inFlight = CompletableDeferred()
            // The old classic connection is unreachable or already gone; the new one gets its own.
            classicAbort = null
            // Launched under the lock, so the loop's ownership check sees reconnectJob already set.
            reconnectJob = reconnectScope.launch { reconnectLoop(policy, cause) }
            Detached(session, clientScope).also {
                session = null
                clientScope = null
            }
        }
        failPendingInvokes("Connection was lost", cause)
        log(SignalRLogLevel.INFO, cause) { "Connection lost; HubConnection is now RECONNECTING." }
        release(detached)
    }

    /**
     * Retries [establish] until it succeeds, [policy] gives up, or [disconnect] / [close] cancels
     * this loop. Each attempt runs in a fresh scope that the loop releases if the attempt fails.
     */
    private suspend fun reconnectLoop(policy: RetryPolicy, initialCause: Throwable?) {
        val self = currentCoroutineContext()[Job]
        val lostAtNanos = System.nanoTime()
        var reason = initialCause
        var attempts = 0
        while (true) {
            val delayMs = try {
                policy.nextRetryDelayMs(RetryContext(attempts, elapsedMs(lostAtNanos), reason))
            } catch (t: Throwable) {
                log(SignalRLogLevel.ERROR, t) { "RetryPolicy threw; stopping reconnect." }
                null
            }
            if (delayMs == null) {
                giveUpReconnecting(self, attempts, reason)
                return
            }
            log(SignalRLogLevel.INFO) { "Reconnect attempt ${attempts + 1} in ${delayMs}ms." }
            delay(delayMs.coerceAtLeast(0))

            val scope = synchronized(lock) {
                if (reconnectJob !== self || closed.get()) return
                newConnectionScope().also { clientScope = it }
            }
            attempts++
            try {
                establish(scope)
            } catch (ce: CancellationException) {
                releaseAttempt(scope)
                // Ktor can surface a transport failure as a CancellationException; only a real
                // cancellation of this loop (disconnect / close) ends it.
                if (!currentCoroutineContext().isActive) throw ce
                log(SignalRLogLevel.WARN, ce) { "Reconnect attempt $attempts was cancelled by the transport." }
                reason = ce
                continue
            } catch (abort: ConnectAbortedException) {
                releaseAttempt(scope)
                return
            } catch (t: Throwable) {
                log(SignalRLogLevel.WARN, t) { "Reconnect attempt $attempts failed." }
                reason = t
                releaseAttempt(scope)
                continue
            }
            synchronized(lock) { if (reconnectJob === self) reconnectJob = null }
            log(SignalRLogLevel.INFO) { "Reconnected after $attempts attempt(s)." }
            _reconnectedFlow.tryEmit(Unit)
            return
        }
    }

    /** Releases what a failed reconnect attempt opened, unless a disconnect already took it over. */
    private suspend fun releaseAttempt(scope: CoroutineScope) {
        val detached = synchronized(lock) {
            if (clientScope !== scope) return
            Detached(session, clientScope).also {
                session = null
                clientScope = null
            }
        }
        release(detached)
    }

    private fun giveUpReconnecting(self: Job?, attempts: Int, reason: Throwable?) {
        synchronized(lock) {
            if (reconnectJob !== self) return
            reconnectJob = null
            _connectionState.value = HubConnectionState.DISCONNECTED
            inFlight?.completeExceptionally(ReconnectFailedException(attempts, reason))
            inFlight = null
        }
        log(SignalRLogLevel.WARN, reason) { "Giving up reconnecting after $attempts attempt(s); HubConnection is now DISCONNECTED." }
        notifyError(ReconnectFailedException(attempts, reason))
    }

    /** Best effort, and bounded so a dead network can't stall [disconnect] for long. */
    private suspend fun sendClassicAbort(abort: ClassicAbort) {
        log(SignalRLogLevel.DEBUG) { "Sending classic abort request..." }
        log(SignalRLogLevel.VERBOSE) { "Classic abort URL: ${abort.url}" }
        try {
            val response = withTimeoutOrNull(CLASSIC_ABORT_TIMEOUT_MS.milliseconds) {
                client.post(abort.url) { abort.headers.forEach { entry -> header(entry.key, entry.value) } }
            }
            if (response == null) {
                log(SignalRLogLevel.DEBUG) { "Classic abort got no answer within ${CLASSIC_ABORT_TIMEOUT_MS}ms." }
            } else {
                log(SignalRLogLevel.DEBUG) { "Classic abort answered ${response.status}." }
            }
        } catch (t: Throwable) {
            log(SignalRLogLevel.DEBUG) { "Classic abort failed: ${t::class.simpleName}" }
        }
    }

    private suspend fun WebSocketSession.closeQuietly() {
        try {
            close()
        } catch (t: Throwable) {
            log(SignalRLogLevel.VERBOSE) { "Error closing WebSocket session: ${t.message}" }
        }
    }

    private fun formatOutgoingMessage(method: String, args: Array<out Any?>, invocationId: String?): String {
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

    override suspend fun send(method: String, vararg args: Any?) {
        val activeSession = requireConnected(method)
        val frameText = formatOutgoingMessage(method, args, null)
        log(SignalRLogLevel.DEBUG) { "Sending '${describeMethod(method)}'." }
        log(SignalRLogLevel.VERBOSE) { "Outgoing frame: $frameText" }
        sendText(activeSession, frameText)
    }

    override suspend fun invoke(method: String, vararg args: Any?, timeoutMs: Long?): JsonElement {
        require(timeoutMs == null || timeoutMs >= 0) { "timeoutMs must be >= 0, was $timeoutMs" }
        val effectiveTimeoutMs = timeoutMs ?: invocationTimeoutMs
        val activeSession = requireConnected(method)
        val id = invocationIdCounter.getAndIncrement().toString()
        val deferred = CompletableDeferred<JsonElement>()
        pendingInvokes[id] = deferred

        return try {
            // The connection may have dropped between the check above and registering the call,
            // after teardown already failed the pending calls; this one would then wait forever.
            if (session !== activeSession || _connectionState.value != HubConnectionState.CONNECTED) {
                throw ConnectionLostException("Connection was lost before invocation '$id' was sent.")
            }
            val frameText = formatOutgoingMessage(method, args, id)
            log(SignalRLogLevel.DEBUG) { "Invoking '${describeMethod(method)}' (id=$id)." }
            log(SignalRLogLevel.VERBOSE) { "Outgoing invocation frame (id=$id): $frameText" }
            sendText(activeSession, frameText)
            if (effectiveTimeoutMs > 0) {
                withTimeoutOrNull(effectiveTimeoutMs.milliseconds) { deferred.await() }
                    ?: throw InvocationTimeoutException(describeMethod(method), effectiveTimeoutMs)
            } else {
                deferred.await()
            }
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
