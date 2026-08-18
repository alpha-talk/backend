package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.lang.management.ManagementFactory
import java.time.Duration

@Component
@ConditionalOnKisAccounts
class RedisMinuteRefreshLock(
    private val redis: StringRedisTemplate,
    private val instanceId: String = ManagementFactory.getRuntimeMXBean().name,
) : MinuteRefreshLock {
    override fun tryAcquire(code: String, ttl: Duration): Boolean =
        redis.opsForValue().setIfAbsent(Keys.minuteRefreshLock(code), instanceId, ttl) == true

    override fun release(code: String) {
        redis.execute(RELEASE_SCRIPT, listOf(Keys.minuteRefreshLock(code)), instanceId)
    }

    companion object {
        private val RELEASE_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
