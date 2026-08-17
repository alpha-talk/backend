package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
@ConditionalOnKisAccounts
class RedisMinuteMarketDivStore(
    private val redis: StringRedisTemplate,
    private val ttl: Duration = Duration.ofDays(2),
) : MinuteMarketDivStore {
    override fun get(code: String, date: String): String? =
        redis.opsForValue().get(Keys.minuteMarketDiv(code, date))

    override fun put(code: String, date: String, div: String) {
        redis.opsForValue().set(Keys.minuteMarketDiv(code, date), div, ttl)
    }
}
