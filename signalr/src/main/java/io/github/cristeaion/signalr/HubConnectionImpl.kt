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
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds

internal class HubConnectionImpl(
    private val client: HttpClient,
    private val baseUrl: String,
    private val connectionHeaders: Map<String, String>,
    private val accessTokenProvider: (suspend () -> String?)?,
    private val skipNegotiate: Boolean = false,
    private val logger: SignalRLogger = ConsoleSignalRLogger(),
    private val handshakeTimeoutMs: Long = 15_000L
) : HubConnection {

    private val _connectionState = MutableStateFlow(HubConnectionState.DISCONNECTED)
    override val connectionState: StateFlow<HubConnectionState> = _connectionState.asStateFlow()

    private val _errorFlow = MutableSharedFlow<Throwable>(extraBufferCapacity = 64)
    override val errorFlow: SharedFlow<Throwable> = _errorFlow.asSharedFlow()

    private val _eventsFlow = MutableSharedFlow<SignalREvent>(extraBufferCapacity = 128)
    override val eventsFlow: SharedFlow<SignalREvent> = _eventsFlow.asSharedFlow()

    private val isClassic = isClassicSignalRUrl(baseUrl)
    private val classicHubNames = extractClassicHubNames(baseUrl)

    private var session: WebSocketSession? = null
    private var clientScope: CoroutineScope? = null
    private val invocationIdCounter = AtomicInteger(1)

    /** Keyed by lowercase method name - hub method matching is case-insensitive. */
    private val callbacks = ConcurrentHashMap<String, CopyOnWriteArrayList<(List<JsonElement>) -> Unit>>()
    private val pendingInvokes = ConcurrentHashMap<String, CompletableDeferred<JsonElement>>()

    private fun notifyError(throwable: Throwable) {
        _errorFlow.tryEmit(throwable)
    }


    override suspend fun connect() {
        if (_connectionState.value == HubConnectionState.CONNECTED) {
            logger.log(SignalRLogLevel.INFO, "HubConnection", "Connect skipped: HubConnection is already CONNECTED.")
            return
        }
        _connectionState.value = HubConnectionState.CONNECTING
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Initiating connection to $baseUrl (skipNegotiate=$skipNegotiate)...")

        val token = accessTokenProvider?.invoke()
        val headersWithAuth = connectionHeaders.toMutableMap()
        if (!headersWithAuth.containsKey("User-Agent")) {
            headersWithAuth["User-Agent"] = "Mozilla/5.0 (Android; EasyMarkets SignalR)"
        }
        if (token != null) {
            headersWithAuth["Authorization"] = "Bearer $token"
            logger.log(SignalRLogLevel.VERBOSE, "HubConnection", "Authorization token injected.")
        }

        var negotiateResponse: NegotiateResponse? = null
        if (!skipNegotiate) {
            try {
                negotiateResponse = negotiate(baseUrl, headersWithAuth)
                if (negotiateResponse.error != null) {
                    val err = RuntimeException("Negotiate error: ${negotiateResponse.error}")
                    notifyError(err)
                    throw err
                }
                logger.log(
                    SignalRLogLevel.INFO,
                    "HubConnection",
                    "Negotiation successful. Token=${negotiateResponse.getEffectiveConnectionToken()}"
                )
            } catch (t: Throwable) {
                logger.log(SignalRLogLevel.ERROR, "HubConnection", "Negotiate failed for $baseUrl", t)
                _connectionState.value = HubConnectionState.DISCONNECTED
                notifyError(t)
                throw t
            }
        } else {
            logger.log(SignalRLogLevel.INFO, "HubConnection", "Skipping negotiation phase as requested.")
        }

        val wsUrl = buildWebSocketUrl(baseUrl, negotiateResponse, token)
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Opening WebSocket connection to $wsUrl")

        try {
            val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
            clientScope = scope

            session = client.webSocketSession(urlString = wsUrl) {
                headersWithAuth.forEach { entry -> header(entry.key, entry.value) }
            }

            if (isClassic) {
                // Classic servers cannot deserialize the Core handshake frame, so it must not be
                // sent. They instead push an init frame ({"S":1,...}) and then wait for /start.
                val initFrame = withTimeoutOrNull(handshakeTimeoutMs.milliseconds) {
                    session?.incoming?.receive() as? Frame.Text
                }
                if (initFrame != null) {
                    val frameText = initFrame.readText()
                    logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Received classic init frame: $frameText")
                    handleIncomingPayload(frameText)
                } else {
                    logger.log(SignalRLogLevel.WARN, "HubConnection", "No classic init frame arrived within ${handshakeTimeoutMs}ms; continuing to /start anyway.")
                }
                startClassicConnection(negotiateResponse, headersWithAuth)
            } else {
                try {
                    logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Sending SignalR handshake request frame...")
                    session?.send(Frame.Text(SignalRProtocol.createHandshakeMessage()))
                    val firstFrame = withTimeoutOrNull(handshakeTimeoutMs.milliseconds) {
                        session?.incoming?.receive() as? Frame.Text
                    }
                    if (firstFrame != null) {
                        val frameText = firstFrame.readText()
                        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Received initial handshake response frame: $frameText")
                        val handshakeFrames = SignalRProtocol.parseFrames(frameText)
                        if (handshakeFrames.isNotEmpty()) {
                            val handshakeResponse = SignalRProtocol.json.decodeFromString<HandshakeResponse>(handshakeFrames[0])
                            if (handshakeResponse.error != null) {
                                val err = IllegalStateException("Handshake failed: ${handshakeResponse.error}")
                                notifyError(err)
                                throw err
                            }
                            logger.log(SignalRLogLevel.DEBUG, "HubConnection", "ASP.NET Core SignalR handshake completed successfully.")
                        }
                    }
                } catch (e: IllegalStateException) {
                    throw e
                } catch (e: Exception) {
                    logger.log(SignalRLogLevel.WARN, "HubConnection", "Handshake step failed: ${e.message}")
                }
            }

            _connectionState.value = HubConnectionState.CONNECTED
            logger.log(SignalRLogLevel.INFO, "HubConnection", "WebSocket connected successfully! Connection state is now CONNECTED.")

            scope.launch {
                logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Started background WebSocket frame reader loop.")
                try {
                    for (frame in session!!.incoming) {
                        if (frame is Frame.Text) {
                            handleIncomingPayload(frame.readText())
                        }
                    }
                } catch (ce: CancellationException) {
                    logger.log(SignalRLogLevel.INFO, "HubConnection", "WebSocket frame reader cancelled gracefully.")
                } catch (t: Throwable) {
                    logger.log(SignalRLogLevel.ERROR, "HubConnection", "WebSocket frame reader error", t)
                    notifyError(t)
                } finally {
                    logger.log(SignalRLogLevel.INFO, "HubConnection", "WebSocket frame reader loop terminated.")
                    disconnectInternal()
                }
            }

        } catch (t: Throwable) {
            logger.log(SignalRLogLevel.ERROR, "HubConnection", "Failed to establish WebSocket session with $wsUrl", t)
            _connectionState.value = HubConnectionState.DISCONNECTED
            notifyError(t)
            throw t
        }
    }

    private suspend fun startClassicConnection(
        negotiateResponse: NegotiateResponse?,
        headers: Map<String, String>,
    ) {
        val startUrl = getStartUrl(baseUrl, negotiateResponse?.getEffectiveConnectionToken())
        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Sending classic start request to $startUrl")
        val response = try {
            client.get(startUrl) {
                headers.forEach { entry -> header(entry.key, entry.value) }
            }
        } catch (t: Throwable) {
            logger.log(SignalRLogLevel.ERROR, "HubConnection", "Classic start request failed", t)
            notifyError(t)
            throw t
        }

        val body = response.bodyAsText()
        if (response.status.value !in 200..299) {
            val err = RuntimeException("Classic start failed with status ${response.status}: $body")
            logger.log(SignalRLogLevel.ERROR, "HubConnection", "Classic start rejected", err)
            notifyError(err)
            throw err
        }
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Classic start acknowledged: $body")
    }

    private suspend fun negotiate(baseUrl: String, headers: Map<String, String>): NegotiateResponse {
        val negotiateUrl = getNegotiateUrl(baseUrl)
        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Sending negotiate POST request to $negotiateUrl")

        val response = client.post(negotiateUrl) {
            headers.forEach { entry -> header(entry.key, entry.value) }
            parameter("negotiateVersion", "1")
            parameter("clientProtocol", "1.5")
        }

        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Negotiate response HTTP status: ${response.status}")

        if (response.status.value !in 200..299) {
            val err = RuntimeException("Negotiate failed with status ${response.status}")
            notifyError(err)
            throw err
        }

        val responseText = response.bodyAsText()
        logger.log(SignalRLogLevel.VERBOSE, "HubConnection", "Raw Negotiate response payload: $responseText")
        return SignalRProtocol.json.decodeFromString<NegotiateResponse>(responseText)
    }

    private fun dispatchCallbacks(target: String, args: List<JsonElement>) {
        _eventsFlow.tryEmit(SignalREvent(target, args))

        val handlers = callbacks[target.lowercase()] ?: return
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Dispatching '$target' to ${handlers.size} handler(s)")
        handlers.forEach { handler ->
            try {
                handler(args)
            } catch (t: Throwable) {
                logger.log(SignalRLogLevel.ERROR, "HubConnection", "Handler for '$target' threw exception", t)
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
                logger.log(SignalRLogLevel.INFO, "HubConnection", "Result for '$id' (nothing awaiting it): $value")
            }
        }
    }

    private fun handleIncomingPayload(payload: String) {
        logger.log(SignalRLogLevel.VERBOSE, "HubConnection", "Handling incoming payload (${payload.length} chars): $payload")
        val frames = SignalRProtocol.parseFrames(payload)
        for (frameText in frames) {
            try {
                val msg = SignalRProtocol.json.decodeFromString<HubMessageJson>(frameText)
                if (msg.type != 0 || msg.target != null) {
                    logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Parsed HubMessageJson (type=${msg.type}, target=${msg.target}, invocationId=${msg.invocationId})")
                    when (msg.type) {
                        1 -> { // Invocation
                            val target = msg.target ?: continue
                            val args = msg.arguments ?: emptyList()
                            dispatchCallbacks(target, args)
                        }

                        3 -> { // Completion
                            val id = msg.invocationId ?: continue
                            logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Received completion for invocationId '$id'. Error=${msg.error}")
                            completeInvocation(id, msg.result, msg.error)
                        }

                        6 -> { // Ping frame
                            logger.log(SignalRLogLevel.VERBOSE, "HubConnection", "Received Ping frame (type 6). Responding with Pong...")
                            clientScope?.launch {
                                session?.send(Frame.Text("{\"type\":6}"))
                            }
                        }
                    }
                    continue
                }
            } catch (_: Throwable) {
                // Fall back to Classic ASP.NET SignalR frame parsing
            }

            try {
                val classicFrame = SignalRProtocol.json.decodeFromString<ClassicHubFrame>(frameText)
                logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Parsed ClassicHubFrame with ${classicFrame.M?.size ?: 0} invocation(s).")

                // Classic servers answer an invocation with {"I":"<id>","R":<result>} / {"I":..,"E":..}
                // rather than a Core type-3 completion message.
                val completionId = classicFrame.I
                if (completionId != null) {
                    logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Received classic completion for invocationId '$completionId'. Error=${classicFrame.E}")
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
                logger.log(SignalRLogLevel.WARN, "HubConnection", "Unrecognized frame payload: $frameText")
            }
        }
    }

    override suspend fun disconnect() {
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Disconnect requested by caller.")
        disconnectInternal()
    }

    private suspend fun disconnectInternal() {
        if (_connectionState.value == HubConnectionState.DISCONNECTED) return
        _connectionState.value = HubConnectionState.DISCONNECTED
        logger.log(SignalRLogLevel.INFO, "HubConnection", "Disconnecting session and cancelling client coroutine scope...")
        clientScope?.cancel()
        try {
            session?.close()
        } catch (t: Throwable) {
            logger.log(SignalRLogLevel.VERBOSE, "HubConnection", "Error closing WebSocket session: ${t.message}")
        }
        session = null
        clientScope = null
        pendingInvokes.values.forEach { it.cancel() }
        pendingInvokes.clear()
        logger.log(SignalRLogLevel.INFO, "HubConnection", "HubConnection is now DISCONNECTED.")
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
        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Sending frame: $frameText")
        activeSession.send(Frame.Text(frameText))
    }

    override suspend fun invoke(method: String, vararg args: Any): JsonElement {
        val activeSession = requireConnected(method)
        val id = invocationIdCounter.getAndIncrement().toString()
        val deferred = CompletableDeferred<JsonElement>()
        pendingInvokes[id] = deferred

        return try {
            val frameText = formatOutgoingMessage(method, args, id)
            logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Sending invocation frame (id=$id): $frameText")
            activeSession.send(Frame.Text(frameText))
            deferred.await()
        } catch (t: Throwable) {
            logger.log(SignalRLogLevel.ERROR, "HubConnection", "Invocation '$method' (id=$id) failed", t)
            throw t
        } finally {
            pendingInvokes.remove(id)
        }
    }

    private fun requireConnected(method: String): WebSocketSession {
        val activeSession = session
        if (_connectionState.value != HubConnectionState.CONNECTED || activeSession == null) {
            throw IllegalStateException("Cannot call '$method': connection is ${_connectionState.value}.")
        }
        return activeSession
    }

    override fun on(method: String, handler: (List<JsonElement>) -> Unit): Subscription {
        logger.log(SignalRLogLevel.DEBUG, "HubConnection", "Registered handler for '$method'")
        val handlers = callbacks.getOrPut(method.lowercase()) { CopyOnWriteArrayList() }
        handlers.add(handler)
        return Subscription { handlers.remove(handler) }
    }
}
