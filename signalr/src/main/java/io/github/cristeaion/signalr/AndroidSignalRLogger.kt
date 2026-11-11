package io.github.cristeaion.signalr

import android.util.Log

/** Writes to Logcat. */
public class AndroidSignalRLogger(
    private val minLevel: SignalRLogLevel = SignalRLogLevel.DEBUG
) : SignalRLogger {
    override fun isLoggable(level: SignalRLogLevel): Boolean = level.isAtLeast(minLevel)

    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        if (!isLoggable(level)) return

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
