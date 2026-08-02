package com.alphatalk.coreapi.support

import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration

@Component
class RedisRateLimiter(
    private val redis: StringRedisTemplate,
    private val clock: Clock = Clock.systemUTC(),
) : RateLimiter {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun tryAcquire(action: String, key: String, limit: Int, window: Duration): RateLimitDecision {
        require(limit > 0) { "limit must be positive" }
        val windowSeconds = window.seconds
        require(windowSeconds > 0) { "window must be at least a second" }
        val nowSeconds = clock.instant().epochSecond
        val windowIndex = nowSeconds / windowSeconds
        val windowKey = "rl:$action:$key:$windowIndex"
        val count = runCatching {
            redis.execute(INCR_WITH_TTL, listOf(windowKey), windowSeconds.toString())
        }.getOrElse {
            log.warn("rate limit counter unavailable, allowing request: action={}", action, it)
            return RateLimitDecision(allowed = true, retryAfterSeconds = 0)
        } ?: return RateLimitDecision(allowed = true, retryAfterSeconds = 0)
        val retryAfter = (windowIndex + 1) * windowSeconds - nowSeconds
        return RateLimitDecision(allowed = count <= limit, retryAfterSeconds = retryAfter)
    }

    companion object {
        private val INCR_WITH_TTL = DefaultRedisScript(
            """
            local count = redis.call('INCR', KEYS[1])
            if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
            return count
            """.trimIndent(),
            Long::class.javaObjectType,
        )
    }
}
