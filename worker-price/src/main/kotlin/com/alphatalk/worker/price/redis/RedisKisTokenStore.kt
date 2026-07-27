package com.alphatalk.worker.price.redis

import com.alphatalk.kis.auth.KisTokenStore
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.UUID

@Component
class RedisKisTokenStore(
    private val redis: StringRedisTemplate,
) : KisTokenStore {
    override fun get(keyId: String): String? = redis.opsForValue().get(tokenKey(keyId))

    override fun put(keyId: String, token: String, ttl: Duration) {
        redis.opsForValue().set(tokenKey(keyId), token, ttl)
    }

    override fun evict(keyId: String) {
        redis.delete(tokenKey(keyId))
    }

    override fun tryLock(keyId: String, ttl: Duration): String? {
        val lockToken = UUID.randomUUID().toString()
        val acquired = redis.opsForValue().setIfAbsent(lockKey(keyId), lockToken, ttl) == true
        return if (acquired) lockToken else null
    }

    override fun unlock(keyId: String, lockToken: String) {
        redis.execute(COMPARE_DELETE, listOf(lockKey(keyId)), lockToken)
    }

    override fun lastIssuedAt(keyId: String): Instant? =
        redis.opsForValue().get(issuedKey(keyId))?.toLongOrNull()?.let(Instant::ofEpochMilli)

    override fun markIssued(keyId: String, at: Instant) {
        redis.opsForValue().set(issuedKey(keyId), at.toEpochMilli().toString())
    }

    private fun tokenKey(keyId: String) = "kis:token:$keyId"

    private fun lockKey(keyId: String) = "kis:token:lock:$keyId"

    private fun issuedKey(keyId: String) = "kis:token:issued:$keyId"

    companion object {
        private val COMPARE_DELETE = DefaultRedisScript(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long::class.java,
        )
    }
}
