package io.github.cristeaion.signalr

/** Discards all output. */
object NoOpSignalRLogger : SignalRLogger {
    override fun isLoggable(level: SignalRLogLevel): Boolean = false

    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        // No-op
    }
}
