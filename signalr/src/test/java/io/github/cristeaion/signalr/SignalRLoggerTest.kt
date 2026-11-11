package io.github.cristeaion.signalr

import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SignalRLoggerTest {

    private class RecordingLogger : SignalRLogger {
        val messages = mutableListOf<String>()
        override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
            messages += "$level:$message"
        }
    }

    @Test
    fun lazyMessageIsNotBuiltBelowTheMinimumLevel() {
        val delegate = RecordingLogger()
        val logger = LevelFilteringSignalRLogger(delegate, SignalRLogLevel.INFO)
        var built = 0

        logger.log(SignalRLogLevel.VERBOSE, "T") { built++; "verbose" }
        logger.log(SignalRLogLevel.DEBUG, "T") { built++; "debug" }
        logger.log(SignalRLogLevel.WARN, "T") { built++; "warn" }

        assertEquals(1, built)
        assertEquals(listOf("WARN:warn"), delegate.messages)
    }

    @Test
    fun builtInLoggersReportTheirLevels() {
        val console = ConsoleSignalRLogger(SignalRLogLevel.WARN)
        assertFalse(console.isLoggable(SignalRLogLevel.INFO))
        assertTrue(console.isLoggable(SignalRLogLevel.ERROR))
        assertFalse(console.isLoggable(SignalRLogLevel.NONE))

        assertFalse(NoOpSignalRLogger.isLoggable(SignalRLogLevel.ERROR))
    }

    @Test
    fun customLoggerWithoutOverrideLogsEverythingButNone() {
        val logger = RecordingLogger()
        assertTrue(logger.isLoggable(SignalRLogLevel.VERBOSE))
        assertFalse(logger.isLoggable(SignalRLogLevel.NONE))
    }

    @Test
    fun filteringRespectsTheDelegatesOwnLevel() {
        val logger = LevelFilteringSignalRLogger(ConsoleSignalRLogger(SignalRLogLevel.ERROR), SignalRLogLevel.DEBUG)
        assertFalse(logger.isLoggable(SignalRLogLevel.INFO))
        assertTrue(logger.isLoggable(SignalRLogLevel.ERROR))
    }
}
