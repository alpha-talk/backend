package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Clock

interface WatchlistMirror {
    fun add(userId: Long, code: String)

    fun remove(userId: Long, code: String)
}

interface WatchlistAnnouncer {
    fun announce(userId: Long, added: List<String>, removed: List<String>)
}

class RedisWatchlistBroadcaster(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    private val clock: Clock,
) : WatchlistMirror, WatchlistAnnouncer {
    override fun add(userId: Long, code: String) {
        redis.opsForSet().add(Keys.watchlist(userId), code)
    }

    override fun remove(userId: Long, code: String) {
        redis.opsForSet().remove(Keys.watchlist(userId), code)
    }

    override fun announce(userId: Long, added: List<String>, removed: List<String>) {
        redis.convertAndSend(
            Channels.WATCHLIST_UPDATED,
            mapper.writeValueAsString(WatchlistUpdated(userId, added, removed, clock.millis())),
        )
    }
}
