package com.alphatalk.coreapi.notification

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

interface BadgeCache {
    fun find(userId: Long): BadgeResponse?

    fun store(userId: Long, badge: BadgeResponse)

    fun evict(userId: Long)
}

@Component
class RedisBadgeCache(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
) : BadgeCache {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun find(userId: Long): BadgeResponse? = runCatching {
        redis.opsForValue().get(key(userId))?.let { mapper.readValue<BadgeResponse>(it) }
    }.getOrElse {
        log.warn("badge cache read failed: userId={}", userId, it)
        null
    }

    override fun store(userId: Long, badge: BadgeResponse) {
        runCatching {
            redis.opsForValue().set(key(userId), mapper.writeValueAsString(badge), TTL)
        }.onFailure {
            log.warn("badge cache store failed: userId={}", userId, it)
        }
    }

    override fun evict(userId: Long) {
        runCatching {
            redis.delete(key(userId))
        }.onFailure {
            log.warn("badge cache evict failed: userId={}", userId, it)
        }
    }

    private fun key(userId: Long) = "badge:$userId"

    companion object {
        private val TTL: Duration = Duration.ofSeconds(10)
    }
}
