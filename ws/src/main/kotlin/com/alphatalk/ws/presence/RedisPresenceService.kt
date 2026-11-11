package com.alphatalk.ws.presence

import com.alphatalk.contracts.Keys
import com.alphatalk.ws.config.WsProperties
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Repository
import java.time.Duration

@Repository
class RedisPresenceService(
    private val redis: StringRedisTemplate,
    props: WsProperties,
    meters: MeterRegistry,
) : PresenceRegistry {
    private val log = LoggerFactory.getLogger(javaClass)
    private val ttl: Duration = Duration.ofSeconds(props.presence.ttlSeconds)
    private val failures: Map<String, Counter> = listOf("add", "remove", "refresh").associateWith { operation ->
        meters.counter("ws.presence.failures", "operation", operation)
    }

    override fun add(userId: Long, sessionId: String) = bestEffort("add") {
        val key = Keys.presence(userId)
        redis.opsForSet().add(key, sessionId)
        redis.expire(key, ttl)
    }

    override fun remove(userId: Long, sessionId: String) = bestEffort("remove") {
        redis.opsForSet().remove(Keys.presence(userId), sessionId)
    }

    override fun refresh(userIds: Collection<Long>) = bestEffort("refresh") {
        userIds.forEach { redis.expire(Keys.presence(it), ttl) }
    }

    private inline fun bestEffort(op: String, block: () -> Any?) {
        try {
            block()
        } catch (e: Exception) {
            failures.getValue(op).increment()
            log.warn("presence {} failed", op, e)
        }
    }
}
