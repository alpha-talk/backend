package com.alphatalk.worker.price.market

import com.alphatalk.contracts.Keys
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.time.Duration

@Component
@ConditionalOnKisAccounts
class RedisMarketDivStore(
    private val redis: StringRedisTemplate,
    private val ttl: Duration = Duration.ofDays(7),
) : MarketDivStore {
    override fun get(code: String): String? = redis.opsForValue().get(Keys.marketDiv(code))

    override fun confirm(code: String, div: String) {
        redis.opsForValue().set(Keys.marketDiv(code), div, ttl)
    }
}
