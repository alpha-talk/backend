package com.alphatalk.worker.batch.kis

import com.alphatalk.contracts.Keys
import com.alphatalk.kis.auth.KisTokenStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration
import java.time.Instant
import java.util.UUID

class RedisKisTokenStore(
    private val redis: StringRedisTemplate,
) : KisTokenStore {
    override fun get(keyId: String): String? = redis.opsForValue().get(Keys.kisToken(keyId))

    override fun put(keyId: String, token: String, ttl: Duration) {
        redis.opsForValue().set(Keys.kisToken(keyId), token, ttl)
    }

    override fun evict(keyId: String) {
        redis.delete(Keys.kisToken(keyId))
    }

    override fun tryLock(keyId: String, ttl: Duration): String? {
        val lockToken = UUID.randomUUID().toString()
        val acquired = redis.opsForValue().setIfAbsent(Keys.kisTokenLock(keyId), lockToken, ttl) == true
        return if (acquired) lockToken else null
    }

    override fun unlock(keyId: String, lockToken: String) {
        redis.execute(COMPARE_DELETE, listOf(Keys.kisTokenLock(keyId)), lockToken)
    }

    override fun lastIssuedAt(keyId: String): Instant? =
        redis.opsForValue().get(Keys.kisTokenIssued(keyId))?.toLongOrNull()?.let(Instant::ofEpochMilli)

    override fun markIssued(keyId: String, at: Instant) {
        redis.opsForValue().set(Keys.kisTokenIssued(keyId), at.toEpochMilli().toString())
    }

    companion object {
        private val COMPARE_DELETE = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
