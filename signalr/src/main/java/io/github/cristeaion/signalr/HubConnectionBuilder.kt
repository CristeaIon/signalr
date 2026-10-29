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
    private var logger: SignalRLogger? = null
    private var logLevel: SignalRLogLevel = SignalRLogLevel.DEBUG

    companion object {
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
            handshakeTimeoutMs
        )
    }
}
