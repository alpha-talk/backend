package com.alphatalk.worker.price.leader

import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration

class RedisLeaderLock(
    private val redis: StringRedisTemplate,
    private val instanceId: String,
    private val ttl: Duration = Duration.ofSeconds(30),
) : LeaderLock {
    override fun tryAcquire(): Boolean {
        if (redis.opsForValue().setIfAbsent(LEADER_KEY, instanceId, ttl) == true) return true
        return redis.execute(RENEW_SCRIPT, listOf(LEADER_KEY), instanceId, ttl.toMillis().toString()) == 1L
    }

    override fun release() {
        redis.execute(RELEASE_SCRIPT, listOf(LEADER_KEY), instanceId)
    }

    companion object {
        const val LEADER_KEY = "worker:price:leader"

        private val RENEW_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('pexpire', KEYS[1], ARGV[2]) else return 0 end",
            Long::class.java,
        )
        private val RELEASE_SCRIPT = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
