package io.github.cristeaion.signalr

import kotlin.math.min
import kotlin.math.pow
import kotlin.math.roundToLong
import kotlin.random.Random

/**
 * Decides whether, and after how long, a dropped connection tries to reconnect. Used by
 * [HubConnectionBuilder.withAutomaticReconnect].
 */
fun interface RetryPolicy {
    /**
     * Returns how many milliseconds to wait before the next reconnect attempt, or `null` to stop
     * reconnecting. Called before every attempt, including the first one.
     */
    fun nextRetryDelayMs(context: RetryContext): Long?

    companion object {
        /** Makes one attempt after each delay in [delaysMs], in order, then gives up. */
        fun fixedDelays(delaysMs: List<Long>): RetryPolicy {
            require(delaysMs.all { it >= 0 }) { "Retry delays must be >= 0, were $delaysMs" }
            val delays = delaysMs.toList()
            return RetryPolicy { context -> delays.getOrNull(context.previousRetryCount) }
        }
    }
}

/** What a [RetryPolicy] knows when it picks the next delay. */
data class RetryContext(
    /** Attempts already made since the connection was lost; 0 before the first one. */
    val previousRetryCount: Int,
    /** Milliseconds since the connection was lost. */
    val elapsedMs: Long,
    /**
     * Why the last attempt failed or, before the first attempt, why the connection dropped.
     * `null` when the server closed the socket without an error.
     */
    val retryReason: Throwable?,
)

/**
 * Exponential backoff with jitter: the n-th attempt (0-based) waits
 * `initialDelayMs * multiplier^n`, capped at [maxDelayMs], then shifted by up to
 * ±[jitterFactor] of itself so that many clients dropped by the same outage don't all
 * reconnect at once.
 *
 * With the defaults it waits about 1s, 2s, 4s, 8s, 16s, then 30s between attempts and never
 * gives up. Limit it with [maxRetries] or [maxElapsedMs], or call `disconnect()` yourself,
 * for example when the app goes to the background.
 */
class ExponentialBackoffRetryPolicy(
    private val initialDelayMs: Long = 1_000,
    private val maxDelayMs: Long = 30_000,
    private val multiplier: Double = 2.0,
    private val jitterFactor: Double = 0.2,
    /** Stop after this many attempts. */
    private val maxRetries: Int = Int.MAX_VALUE,
    /** Stop once this long has passed since the connection was lost; 0 = no limit. */
    private val maxElapsedMs: Long = 0,
    private val random: Random = Random.Default,
) : RetryPolicy {

    init {
        require(initialDelayMs >= 0) { "initialDelayMs must be >= 0, was $initialDelayMs" }
        require(maxDelayMs >= initialDelayMs) { "maxDelayMs ($maxDelayMs) must be >= initialDelayMs ($initialDelayMs)" }
        require(multiplier >= 1.0) { "multiplier must be >= 1, was $multiplier" }
        require(jitterFactor in 0.0..1.0) { "jitterFactor must be in 0..1, was $jitterFactor" }
        require(maxRetries >= 0) { "maxRetries must be >= 0, was $maxRetries" }
        require(maxElapsedMs >= 0) { "maxElapsedMs must be >= 0, was $maxElapsedMs" }
    }

    override fun nextRetryDelayMs(context: RetryContext): Long? {
        if (context.previousRetryCount >= maxRetries) return null
        if (maxElapsedMs > 0 && context.elapsedMs >= maxElapsedMs) return null
        val base = min(maxDelayMs.toDouble(), initialDelayMs * multiplier.pow(context.previousRetryCount))
        val jitter = if (jitterFactor > 0) base * jitterFactor * (random.nextDouble() * 2 - 1) else 0.0
        return (base + jitter).coerceIn(0.0, maxDelayMs.toDouble()).roundToLong()
    }
}
