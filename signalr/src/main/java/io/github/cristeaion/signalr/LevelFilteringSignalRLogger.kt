package io.github.cristeaion.signalr

/** Applies a minimum level to a logger that does no filtering of its own. */
internal class LevelFilteringSignalRLogger(
    private val delegate: SignalRLogger,
    private val minLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
) : SignalRLogger {
    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (level < minLevel || level == SignalRLogLevel.NONE) return
        delegate.log(level, tag, message, throwable)
    }
}
