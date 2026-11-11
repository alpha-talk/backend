package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

fun interface WatchlistBroadcaster {
    fun publish(snapshot: WatchlistUpdated)
}

@Component
class RedisWatchlistBroadcaster(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    meters: MeterRegistry,
) : WatchlistBroadcaster {
    private val log = LoggerFactory.getLogger(javaClass)
    private val failures = meters.counter("core.watchlist.publish.failures")

    override fun publish(snapshot: WatchlistUpdated) {
        try {
            redis.convertAndSend(Channels.WATCHLIST_UPDATED, mapper.writeValueAsString(snapshot))
        } catch (e: Exception) {
            failures.increment()
            log.warn("watchlist publish failed. userId={} rev={}", snapshot.userId, snapshot.rev, e)
        }
    }
}
