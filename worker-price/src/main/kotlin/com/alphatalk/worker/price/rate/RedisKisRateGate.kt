package com.alphatalk.worker.price.rate

import com.alphatalk.contracts.Keys
import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.rate.KisRateGate
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.script.DefaultRedisScript
import java.time.Duration

class RedisKisRateGate(
    private val redis: StringRedisTemplate,
    private val capacity: Int,
    private val refillPerSecond: Double,
    private val acquireTimeout: Duration = Duration.ofSeconds(10),
    private val clock: () -> Long = System::currentTimeMillis,
    private val sleeper: (Long) -> Unit = Thread::sleep,
) : KisRateGate {
    private val retryIntervalMillis: Long = (1_000 / refillPerSecond).toLong().coerceIn(20, 500)

    override fun acquire(keyId: String) {
        val deadline = clock() + acquireTimeout.toMillis()
        while (true) {
            val granted = redis.execute(
                TAKE_TOKEN_SCRIPT,
                listOf(Keys.kisRestRate(keyId)),
                capacity.toString(),
                refillPerSecond.toString(),
            )
            if (granted == 1L) return
            if (clock() >= deadline) {
                throw KisClientException("kis rest gate acquire timeout: keyId=$keyId")
            }
            sleeper(retryIntervalMillis)
        }
    }

    companion object {
        private val TAKE_TOKEN_SCRIPT = DefaultRedisScript(
            """
            local capacity = tonumber(ARGV[1])
            local refill = tonumber(ARGV[2])
            local clock = redis.call('TIME')
            local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
            local state = redis.call('HMGET', KEYS[1], 'tokens', 'updatedAt')
            local tokens = tonumber(state[1])
            local updated = tonumber(state[2])
            if tokens == nil or updated == nil then
              tokens = capacity
              updated = now
            end
            local elapsed = now - updated
            if elapsed < 0 then elapsed = 0 end
            tokens = math.min(capacity, tokens + elapsed / 1000 * refill)
            local granted = 0
            if tokens >= 1 then
              tokens = tokens - 1
              granted = 1
            end
            redis.call('HSET', KEYS[1], 'tokens', tostring(tokens), 'updatedAt', tostring(now))
            redis.call('PEXPIRE', KEYS[1], 120000)
            return granted
            """.trimIndent(),
            Long::class.java,
        )
    }
}
