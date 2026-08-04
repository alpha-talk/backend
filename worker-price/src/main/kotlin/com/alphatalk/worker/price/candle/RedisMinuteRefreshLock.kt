package com.alphatalk.worker.price.candle

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration

class RedisMinuteRefreshLock(
    private val redis: StringRedisTemplate,
    private val instanceId: String,
) : MinuteRefreshLock {
    override fun tryAcquire(code: String, ttl: Duration): Boolean =
        redis.opsForValue().setIfAbsent(key(code), instanceId, ttl) == true

    override fun release(code: String) {
        redis.execute(RELEASE_SCRIPT, listOf(key(code)), instanceId)
    }

    private fun key(code: String) = "$KEY_PREFIX$code"

    companion object {
        const val KEY_PREFIX = "worker:price:minute-refresh:"

        private val RELEASE_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
