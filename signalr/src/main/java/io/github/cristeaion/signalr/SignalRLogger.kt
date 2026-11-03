package io.github.cristeaion.signalr

/** Receives the library's diagnostic output. Pass an implementation to [HubConnectionBuilder.withLogger]. */
interface SignalRLogger {
    fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable? = null)
}
