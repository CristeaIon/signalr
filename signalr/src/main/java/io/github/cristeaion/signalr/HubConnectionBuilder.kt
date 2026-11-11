package io.github.cristeaion.signalr

import io.ktor.client.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.websocket.*
import io.ktor.serialization.kotlinx.json.*

class HubConnectionBuilder private constructor(private val url: String) {

    private val headers = mutableMapOf<String, String>()
    private var accessTokenProvider: (suspend () -> String?)? = null
    private var skipNegotiate: Boolean = false
    private var handshakeTimeoutMs: Long = 15_000L
    private var keepAliveIntervalMs: Long = DEFAULT_KEEP_ALIVE_INTERVAL_MS
    private var serverTimeoutMs: Long = DEFAULT_SERVER_TIMEOUT_MS
    private var logger: SignalRLogger? = null
    private var logLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
    private var retryPolicy: RetryPolicy? = null

    companion object {
        /** Same defaults as the official ASP.NET Core clients. */
        const val DEFAULT_KEEP_ALIVE_INTERVAL_MS: Long = 15_000L
        const val DEFAULT_SERVER_TIMEOUT_MS: Long = 30_000L

        fun create(url: String): HubConnectionBuilder {
            return HubConnectionBuilder(url)
        }
    }

    fun withHeader(name: String, value: String): HubConnectionBuilder {
        headers[name] = value
        return this
    }

    fun withHeaders(headers: Map<String, String>): HubConnectionBuilder {
        this.headers.putAll(headers)
        return this
    }

    fun withAccessTokenProvider(provider: suspend () -> String?): HubConnectionBuilder {
        this.accessTokenProvider = provider
        return this
    }

    fun withLogger(logger: SignalRLogger): HubConnectionBuilder {
        this.logger = logger
        return this
    }

    /** Applies to whichever logger is in use - it no longer replaces one set by [withLogger]. */
    fun withLogLevel(logLevel: SignalRLogLevel): HubConnectionBuilder {
        this.logLevel = logLevel
        return this
    }

    /** How long to wait for the server's handshake or init frame before giving up. */
    fun withHandshakeTimeout(timeoutInMilliseconds: Long): HubConnectionBuilder {
        this.handshakeTimeoutMs = timeoutInMilliseconds
        return this
    }

    /**
     * How often to send a ping when nothing else has been sent, so the server knows the client
     * is alive. ASP.NET Core servers only; classic servers keep the connection alive themselves.
     * `0` turns pinging off. Keep it at most half the server's `ClientTimeoutInterval`.
     */
    fun withKeepAliveInterval(intervalInMilliseconds: Long): HubConnectionBuilder {
        require(intervalInMilliseconds >= 0) { "Keep-alive interval must be >= 0, was $intervalInMilliseconds" }
        this.keepAliveIntervalMs = intervalInMilliseconds
        return this
    }

    /**
     * How long the connection may go without receiving anything before it is treated as lost:
     * it is closed, the state becomes DISCONNECTED and a [ServerTimeoutException] reaches
     * [HubConnection.errorFlow]. Keep it at least twice the server's `KeepAliveInterval`.
     * Classic servers announce their own `KeepAliveTimeout` during negotiate, which wins.
     * `0` turns the timeout off.
     */
    fun withServerTimeout(timeoutInMilliseconds: Long): HubConnectionBuilder {
        require(timeoutInMilliseconds >= 0) { "Server timeout must be >= 0, was $timeoutInMilliseconds" }
        this.serverTimeoutMs = timeoutInMilliseconds
        return this
    }

    /**
     * Reconnects automatically when an established connection drops (socket error, server
     * close, server timeout), using exponential backoff with jitter: about 1s, 2s, 4s, 8s, 16s,
     * then every 30s, with no limit on attempts. While it retries the state is RECONNECTING.
     * It stops on [HubConnection.disconnect] or [HubConnection.close], or when an ASP.NET Core
     * server closes the connection without allowing reconnect. A failed first [HubConnection.connect]
     * is not retried.
     */
    fun withAutomaticReconnect(): HubConnectionBuilder =
        withAutomaticReconnect(ExponentialBackoffRetryPolicy())

    /**
     * Reconnects automatically, making one attempt after each of [delaysMs] in turn and then giving
     * up: the state becomes DISCONNECTED and a [ReconnectFailedException] reaches
     * [HubConnection.errorFlow]. `listOf(0, 2_000, 10_000, 30_000)` matches the official clients.
     */
    fun withAutomaticReconnect(delaysMs: List<Long>): HubConnectionBuilder =
        withAutomaticReconnect(RetryPolicy.fixedDelays(delaysMs))

    /** Reconnects automatically, as [policy] decides. See [ExponentialBackoffRetryPolicy]. */
    fun withAutomaticReconnect(policy: RetryPolicy): HubConnectionBuilder {
        this.retryPolicy = policy
        return this
    }

    fun shouldSkipNegotiate(skipNegotiate: Boolean): HubConnectionBuilder {
        this.skipNegotiate = skipNegotiate
        return this
    }

    fun build(): HubConnection {
        val client = HttpClient(OkHttp) {
            install(WebSockets)
            install(ContentNegotiation) {
                json(SignalRProtocol.json)
            }
        }
        val effectiveLogger = logger
            ?.let { LevelFilteringSignalRLogger(it, logLevel) }
            ?: ConsoleSignalRLogger(logLevel)
        return HubConnectionImpl(
            client,
            url,
            headers,
            accessTokenProvider,
            skipNegotiate,
            effectiveLogger,
            handshakeTimeoutMs,
            keepAliveIntervalMs,
            serverTimeoutMs,
            retryPolicy,
        )
    }
}
