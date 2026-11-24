package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.envelope.WatchlistUpdated
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.springframework.data.redis.RedisConnectionFailureException
import kotlin.test.assertEquals

class RedisWatchlistBroadcasterTest {
    @Test
    fun `Redis failure keeps the committed revision pending for retry`() {
        val redis = mock(RedisWatchlistSnapshots::class.java)
        val revisions = mock(WatchlistRevisionJpaRepository::class.java)
        val reader = mock(WatchlistSnapshotReader::class.java)
        val meters = SimpleMeterRegistry()
        val broadcaster = RedisWatchlistBroadcaster(redis, revisions, reader, meters)
        val snapshot = WatchlistUpdated(1, 14, emptyList())
        doThrow(RedisConnectionFailureException("offline")).`when`(redis).publish(snapshot)

        broadcaster.publish(snapshot)

        verify(revisions, never()).clearRedisPending(snapshot.userId, snapshot.rev)
        assertEquals(1.0, meters.counter("core.watchlist.publish.failures").count())
    }
}
