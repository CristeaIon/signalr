package io.github.cristeaion.signalr

/**
 * Writes to standard output. The default when no logger is supplied, at the builder's log level
 * (WARN unless changed). On Android, [AndroidSignalRLogger] writes to logcat with proper tags.
 */
public class ConsoleSignalRLogger(
    private val minLevel: SignalRLogLevel = SignalRLogLevel.WARN
) : SignalRLogger {
    override fun isLoggable(level: SignalRLogLevel): Boolean = level.isAtLeast(minLevel)

    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (!isLoggable(level)) return

        val msg = "[$level][$tag] $message"
        if (throwable != null) {
            println("$msg\n${throwable.stackTraceToString()}")
        } else {
            println(msg)
        }
    }
}
