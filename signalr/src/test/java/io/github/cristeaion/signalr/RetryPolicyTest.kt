package io.github.cristeaion.signalr

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test
import kotlin.random.Random

class RetryPolicyTest {

    private fun ctx(retries: Int, elapsedMs: Long = 0) = RetryContext(retries, elapsedMs, null)

    @Test
    fun backoffDoublesUpToTheCap() {
        val policy = ExponentialBackoffRetryPolicy(initialDelayMs = 1_000, maxDelayMs = 10_000, jitterFactor = 0.0)

        assertEquals(listOf(1_000L, 2_000L, 4_000L, 8_000L, 10_000L, 10_000L), (0..5).map { policy.nextRetryDelayMs(ctx(it)) })
        assertEquals(10_000L, policy.nextRetryDelayMs(ctx(10_000))) // no overflow
    }

    @Test
    fun jitterStaysWithinItsFactorAndTheCap() {
        val policy = ExponentialBackoffRetryPolicy(
            initialDelayMs = 1_000, maxDelayMs = 4_000, jitterFactor = 0.2, random = Random(42),
        )
        repeat(200) {
            val first = policy.nextRetryDelayMs(ctx(0))!!
            assertTrue("$first", first in 800..1_200)
            val capped = policy.nextRetryDelayMs(ctx(5))!!
            assertTrue("$capped", capped in 3_200..4_000)
        }
    }

    @Test
    fun backoffStopsAtMaxRetriesAndMaxElapsed() {
        val byCount = ExponentialBackoffRetryPolicy(maxRetries = 2)
        assertTrue(byCount.nextRetryDelayMs(ctx(1)) != null)
        assertNull(byCount.nextRetryDelayMs(ctx(2)))

        val byTime = ExponentialBackoffRetryPolicy(maxElapsedMs = 5_000)
        assertTrue(byTime.nextRetryDelayMs(ctx(3, elapsedMs = 4_999)) != null)
        assertNull(byTime.nextRetryDelayMs(ctx(3, elapsedMs = 5_000)))
    }

    @Test
    fun fixedDelaysAreUsedInOrderThenStop() {
        val policy = RetryPolicy.fixedDelays(listOf(0, 2_000, 10_000))

        assertEquals(listOf(0L, 2_000L, 10_000L, null), (0..3).map { policy.nextRetryDelayMs(ctx(it)) })
    }

    @Test(expected = IllegalArgumentException::class)
    fun negativeFixedDelaysAreRejected() {
        RetryPolicy.fixedDelays(listOf(100, -1))
    }

    @Test
    fun outgoingInvocationsDoNotCarryAllowReconnect() {
        val frame = SignalRProtocol.createInvocationMessage("Send", "1", arrayOf("hi"))
        assertTrue(frame, !frame.contains("allowReconnect"))
    }
}
