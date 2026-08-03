package com.alphatalk.coreapi.support

import java.time.Duration

data class RateLimitDecision(
    val allowed: Boolean,
    val retryAfterSeconds: Long,
)

interface RateLimiter {
    fun tryAcquire(action: String, key: String, limit: Int, window: Duration): RateLimitDecision
}

class RateLimitExceededException(
    action: String,
    val retryAfterSeconds: Long,
) : ApiException(
    ErrorCode.RATE_LIMITED,
    "요청이 너무 잦습니다. 잠시 후 다시 시도해 주세요",
    mapOf("action" to action, "retryAfterSeconds" to retryAfterSeconds),
)
