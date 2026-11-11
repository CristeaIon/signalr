package io.github.cristeaion.signalr

/** Receives the library's diagnostic output. Pass an implementation to [HubConnectionBuilder.withLogger]. */
public interface SignalRLogger {
    public fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable? = null)

    /**
     * Whether a message at [level] would be written. The library checks this before building a
     * message, so a disabled level costs nothing. Override it when [log] filters by level.
     */
    public fun isLoggable(level: SignalRLogLevel): Boolean = level != SignalRLogLevel.NONE
}

/** Builds [message] only when [level] is enabled on this logger. */
public inline fun SignalRLogger.log(
    level: SignalRLogLevel,
    tag: String,
    throwable: Throwable? = null,
    message: () -> String
) {
    if (isLoggable(level)) log(level, tag, message(), throwable)
}

internal fun SignalRLogLevel.isAtLeast(minLevel: SignalRLogLevel): Boolean =
    this != SignalRLogLevel.NONE && this >= minLevel
