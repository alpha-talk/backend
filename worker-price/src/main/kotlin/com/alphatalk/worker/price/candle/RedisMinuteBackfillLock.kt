package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration

class RedisMinuteBackfillLock(
    private val redis: StringRedisTemplate,
    private val instanceId: String,
) : MinuteBackfillLock {
    override fun tryAcquire(code: String, ttl: Duration): Boolean =
        redis.opsForValue().setIfAbsent(Keys.minuteBackfillLock(code), instanceId, ttl) == true

    override fun release(code: String) {
        redis.execute(RELEASE_SCRIPT, listOf(Keys.minuteBackfillLock(code)), instanceId)
    }

    companion object {
        private val RELEASE_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
