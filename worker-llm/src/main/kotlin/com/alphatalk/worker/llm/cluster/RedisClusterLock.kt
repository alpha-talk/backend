package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration
import java.util.UUID

class RedisClusterLock(
    private val redis: StringRedisTemplate,
    private val ttl: Duration,
) : ClusterLock {

    override fun <T> withLock(code: String, action: () -> T): T {
        val key = Keys.clusterLock(code)
        val token = UUID.randomUUID().toString()
        val deadline = System.nanoTime() + ttl.multipliedBy(2).toNanos()
        while (redis.opsForValue().setIfAbsent(key, token, ttl) != true) {
            if (System.nanoTime() > deadline) {
                throw IllegalStateException("cluster lock timeout: $code")
            }
            Thread.sleep(50)
        }
        try {
            return action()
        } finally {
            redis.execute(UNLOCK_SCRIPT, listOf(key), token)
        }
    }

    companion object {
        private val UNLOCK_SCRIPT = DefaultRedisScript(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """.trimIndent(),
            Long::class.java,
        )
    }
}
