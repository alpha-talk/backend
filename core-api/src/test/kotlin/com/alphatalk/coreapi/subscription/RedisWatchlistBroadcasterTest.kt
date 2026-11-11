package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.assertEquals

class RedisWatchlistBroadcasterTest {
    @Test
    fun `publishes the committed snapshot and counts a Redis failure without throwing`() {
        val redis = mock(StringRedisTemplate::class.java)
        val meters = SimpleMeterRegistry()
        val mapper = jacksonObjectMapper()
        val broadcaster = RedisWatchlistBroadcaster(redis, mapper, meters)
        val snapshot = WatchlistUpdated(1, 14, emptyList())
        val payload = mapper.writeValueAsString(snapshot)
        `when`(redis.convertAndSend(Channels.WATCHLIST_UPDATED, payload))
            .thenThrow(RedisConnectionFailureException("offline"))

        broadcaster.publish(snapshot)

        verify(redis).convertAndSend(Channels.WATCHLIST_UPDATED, payload)
        assertEquals(1.0, meters.counter("core.watchlist.publish.failures").count())
    }
}
