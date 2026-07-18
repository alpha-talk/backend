package com.alphatalk.ws.presence

import com.alphatalk.contracts.Keys
import com.alphatalk.ws.config.WsProperties
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
class RedisPresenceService(
    private val redis: StringRedisTemplate,
    props: WsProperties,
) : PresenceRegistry {
    private val log = LoggerFactory.getLogger(javaClass)
    private val ttl: Duration = Duration.ofSeconds(props.presence.ttlSeconds)

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
            log.warn("presence {} failed", op, e)
        }
    }
}
