package com.alphatalk.kis.rate

import com.alphatalk.kis.KisClientException
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import io.github.resilience4j.ratelimiter.RateLimiterRegistry
import java.time.Duration
import kotlin.math.floor

class KisRateLimiters(
    officialCallsPerSecond: Double,
    factor: Double = 0.75,
    acquireTimeout: Duration = Duration.ofSeconds(60),
) {
    val permitsPerSecond: Int = maxOf(1, floor(officialCallsPerSecond * factor).toInt())

    private val registry = RateLimiterRegistry.of(
        RateLimiterConfig.custom()
            .limitForPeriod(permitsPerSecond)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(acquireTimeout)
            .build(),
    )

    fun acquire(keyId: String) {
        if (!registry.rateLimiter(keyId).acquirePermission()) {
            throw KisClientException("rate limit acquire timeout: keyId=$keyId")
        }
    }
}
