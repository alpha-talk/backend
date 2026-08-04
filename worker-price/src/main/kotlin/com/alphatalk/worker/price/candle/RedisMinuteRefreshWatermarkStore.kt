package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Duration

class RedisMinuteRefreshWatermarkStore(
    private val redis: StringRedisTemplate,
    private val ttl: Duration = Duration.ofDays(2),
) : MinuteRefreshWatermarkStore {
    override fun fetchedThrough(code: String, date: String): String? =
        redis.opsForValue().get(Keys.minuteRefreshWatermark(code, date))

    override fun record(code: String, date: String, time: String) {
        val key = Keys.minuteRefreshWatermark(code, date)
        val previous = redis.opsForValue().get(key)
        if (previous != null && previous >= time) return
        redis.opsForValue().set(key, time, ttl)
    }
}
