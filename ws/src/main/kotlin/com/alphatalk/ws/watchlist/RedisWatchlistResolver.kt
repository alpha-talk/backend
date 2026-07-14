package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.Keys
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

@Component
class RedisWatchlistResolver(private val redis: StringRedisTemplate) : WatchlistResolver {
    override fun resolve(userId: Long): Set<String> =
        redis.opsForSet().members(Keys.watchlist(userId)) ?: emptySet()
}
