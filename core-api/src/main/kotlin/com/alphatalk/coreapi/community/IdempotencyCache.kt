package com.alphatalk.coreapi.community

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

interface IdempotencyCache {
    fun <T : Any> find(userId: Long, key: String, type: Class<T>): T?

    fun store(userId: Long, key: String, response: Any)
}

@Component
class RedisIdempotencyCache(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
) : IdempotencyCache {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun <T : Any> find(userId: Long, key: String, type: Class<T>): T? = runCatching {
        redis.opsForValue().get(cacheKey(userId, key))?.let { mapper.readValue(it, type) }
    }.getOrElse {
        log.warn("idempotency cache read failed: userId={}", userId, it)
        null
    }

    override fun store(userId: Long, key: String, response: Any) {
        runCatching {
            redis.opsForValue().setIfAbsent(cacheKey(userId, key), mapper.writeValueAsString(response), TTL)
        }.onFailure {
            log.warn("idempotency cache store failed: userId={}", userId, it)
        }
    }

    private fun cacheKey(userId: Long, key: String) = "idem:$userId:$key"

    companion object {
        private val TTL: Duration = Duration.ofMinutes(10)
    }
}
