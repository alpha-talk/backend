package com.alphatalk.worker.price.session

import kotlin.math.pow

class BackoffPolicy(
    private val initialMillis: Long = 1_000,
    private val maxMillis: Long = 60_000,
    private val multiplier: Double = 2.0,
    private val jitterRatio: Double = 0.2,
    private val random: () -> Double = Math::random,
) {
    fun delayFor(consecutiveFailures: Int): Long {
        val attempt = consecutiveFailures.coerceAtLeast(1)
        val base = (initialMillis * multiplier.pow(attempt - 1)).toLong().coerceAtMost(maxMillis)
        val jitter = (base * jitterRatio * (random() * 2 - 1)).toLong()
        return (base + jitter).coerceAtLeast(0)
    }
}
