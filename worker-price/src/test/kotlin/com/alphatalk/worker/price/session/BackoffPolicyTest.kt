package com.alphatalk.worker.price.session

import kotlin.test.Test
import kotlin.test.assertEquals

class BackoffPolicyTest {
    @Test
    fun `실패 횟수에 따라 지수적으로 늘고 상한에서 멈춘다`() {
        val policy = BackoffPolicy(initialMillis = 1_000, maxMillis = 8_000, jitterRatio = 0.0)

        assertEquals(1_000, policy.delayFor(1))
        assertEquals(2_000, policy.delayFor(2))
        assertEquals(4_000, policy.delayFor(3))
        assertEquals(8_000, policy.delayFor(4))
        assertEquals(8_000, policy.delayFor(10))
    }

    @Test
    fun `지터는 비율 범위 안에서 더해지거나 빠진다`() {
        val plus = BackoffPolicy(initialMillis = 1_000, jitterRatio = 0.2, random = { 1.0 })
        val minus = BackoffPolicy(initialMillis = 1_000, jitterRatio = 0.2, random = { 0.0 })

        assertEquals(1_200, plus.delayFor(1))
        assertEquals(800, minus.delayFor(1))
    }
}
