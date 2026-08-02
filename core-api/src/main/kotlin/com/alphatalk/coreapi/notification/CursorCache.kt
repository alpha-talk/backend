package com.alphatalk.coreapi.notification

import com.alphatalk.contracts.Keys
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component

interface CursorCache {
    fun read(userId: Long, codes: List<String>): Map<String, String>?

    fun advance(userId: Long, code: String, eventId: String)

    fun advanceAll(userId: Long, cursors: Map<String, String>)
}

@Component
class RedisCursorCache(
    private val redis: StringRedisTemplate,
) : CursorCache {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun read(userId: Long, codes: List<String>): Map<String, String>? {
        if (codes.isEmpty()) return emptyMap()
        val values = runCatching {
            redis.opsForValue().multiGet(codes.map { Keys.cursor(userId, it) })
        }.getOrElse {
            log.warn("cursor cache read failed: userId={}", userId, it)
            return null
        } ?: return null
        return codes.zip(values)
            .mapNotNull { (code, value) -> value?.let { code to it } }
            .toMap()
    }

    override fun advance(userId: Long, code: String, eventId: String) {
        runCatching {
            redis.execute(SET_IF_NEWER, listOf(Keys.cursor(userId, code)), eventId)
        }.onFailure {
            log.warn("cursor cache advance failed: userId={} code={}", userId, code, it)
        }
    }

    override fun advanceAll(userId: Long, cursors: Map<String, String>) {
        cursors.forEach { (code, eventId) -> advance(userId, code, eventId) }
    }

    companion object {
        private val SET_IF_NEWER = DefaultRedisScript(
            """
            local cur = redis.call('GET', KEYS[1])
            if cur and cur >= ARGV[1] then return 0 end
            redis.call('SET', KEYS[1], ARGV[1])
            return 1
            """.trimIndent(),
            Long::class.javaObjectType,
        )
    }
}
