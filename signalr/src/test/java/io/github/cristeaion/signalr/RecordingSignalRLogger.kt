package io.github.cristeaion.signalr

import org.junit.Assert.fail
import java.util.concurrent.CopyOnWriteArrayList

/** Captures every line the library logs, including the messages of attached throwables. */
class RecordingSignalRLogger : SignalRLogger {
    data class Entry(val level: SignalRLogLevel, val text: String)

    val entries = CopyOnWriteArrayList<Entry>()

    override fun log(level: SignalRLogLevel, tag: String, message: String, throwable: Throwable?) {
        val causes = generateSequence(throwable) { it.cause }.joinToString(" | ") { it.toString() }
        entries += Entry(level, if (causes.isEmpty()) message else "$message | $causes")
    }

    fun text(level: SignalRLogLevel): String =
        entries.filter { it.level == level }.joinToString("\n") { it.text }

    /** Fails when any [secrets] appear in a line logged at DEBUG or above. */
    fun assertNoneBelowVerbose(secrets: List<String>) {
        val leaks = entries
            .filter { it.level != SignalRLogLevel.VERBOSE }
            .filter { entry -> secrets.any { entry.text.contains(it) } }
        if (leaks.isNotEmpty()) {
            fail("Secrets logged above VERBOSE:\n" + leaks.joinToString("\n") { "[${it.level}] ${it.text}" })
        }
    }
}
