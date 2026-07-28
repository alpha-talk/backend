package com.alphatalk.kis.rate

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class KisRateLimitersTest {
    @Test
    fun `내부 한도는 공식 유량에 계수를 곱해 내림한 값이다`() {
        assertEquals(15, KisRateLimiters(20.0, 0.75).permitsPerSecond)
        assertEquals(1, KisRateLimiters(2.0, 0.75).permitsPerSecond)
        assertEquals(1, KisRateLimiters(0.5, 0.75).permitsPerSecond)
    }

    @Test
    fun `한도 초과 호출은 다음 윈도까지 대기한다`() {
        val limiters = KisRateLimiters(2.0, 1.0)
        val start = System.nanoTime()

        repeat(3) { limiters.acquire("k") }

        val elapsedMs = (System.nanoTime() - start) / 1_000_000
        assertTrue(elapsedMs >= 900, "elapsed=${elapsedMs}ms")
    }

    @Test
    fun `계정별로 독립된 한도를 가진다`() {
        val limiters = KisRateLimiters(2.0, 1.0)
        val start = System.nanoTime()

        repeat(2) { limiters.acquire("a") }
        repeat(2) { limiters.acquire("b") }

        val elapsedMs = (System.nanoTime() - start) / 1_000_000
        assertTrue(elapsedMs < 900, "elapsed=${elapsedMs}ms")
    }
}
