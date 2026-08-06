package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

class RedisMinuteMarketDivStore(
    private val redis: StringRedisTemplate,
    private val ttl: Duration = Duration.ofDays(7),
) : MinuteMarketDivStore {
    override fun get(code: String): String? = redis.opsForValue().get(Keys.minuteMarketDiv(code))

    override fun put(code: String, div: String) {
        redis.opsForValue().set(Keys.minuteMarketDiv(code), div, ttl)
    }
}
