package io.github.cristeaion.signalr

import android.util.Log

enum class SignalRLogLevel {
    VERBOSE, DEBUG, INFO, WARN, ERROR, NONE
}

interface SignalRLogger {
    fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable? = null)
}

class AndroidSignalRLogger(
    private val minLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
) : SignalRLogger {
    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (level < minLevel || level == SignalRLogLevel.NONE) return

        when (level) {
            SignalRLogLevel.VERBOSE -> Log.v(tag, message, throwable)
            SignalRLogLevel.DEBUG -> Log.d(tag, message, throwable)
            SignalRLogLevel.INFO -> Log.i(tag, message, throwable)
            SignalRLogLevel.WARN -> Log.w(tag, message, throwable)
            SignalRLogLevel.ERROR -> Log.e(tag, message, throwable)
            SignalRLogLevel.NONE -> {}
        }
    }
}

class ConsoleSignalRLogger(
    private val minLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
) : SignalRLogger {
    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (level < minLevel || level == SignalRLogLevel.NONE) return

        val msg = "[$level][$tag] $message"
        if (throwable != null) {
            println("$msg\n${throwable.stackTraceToString()}")
        } else {
            println(msg)
        }
    }
}

/** Applies a minimum level to a logger that does no filtering of its own. */
class LevelFilteringSignalRLogger(
    private val delegate: SignalRLogger,
    private val minLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
) : SignalRLogger {
    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (level < minLevel || level == SignalRLogLevel.NONE) return
        delegate.log(level, tag, message, throwable)
    }
}

object NoOpSignalRLogger : SignalRLogger {
    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        // No-op
    }
}
