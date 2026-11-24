package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.verifyNoMoreInteractions
import org.mockito.Mockito.`when`
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations

class RedisWatchlistResolverTest {
    private val redis = mock(StringRedisTemplate::class.java)
    private val values: ValueOperations<String, String> = redisValueOperations()
    private val client = mock(WatchlistSnapshotClient::class.java)
    private val mapper = jacksonObjectMapper()
    private val meters = SimpleMeterRegistry()
    private val resolver = RedisWatchlistResolver(redis, mapper, client, "test-key", meters)

    @Test
    fun `empty watchlist is a cache hit even when the internal key is unset`() {
        val cached = WatchlistUpdated(1, 15, emptyList())
        `when`(values.multiGet(listOf(Keys.watchlistSnapshot(1)))).thenReturn(listOf(mapper.writeValueAsString(cached)))
        val redisOnly = RedisWatchlistResolver(redis, mapper, client, "", meters)

        assertThat(redisOnly.resolve(listOf(1))).containsExactly(cached)
        verifyNoInteractions(client)
    }

    @Test
    fun `only keys missing from a mixed batch use the internal API`() {
        val cached = WatchlistUpdated(2, 14, listOf("005930"))
        val recovered = WatchlistUpdated(1, 20, listOf("000660"))
        batchWithMisses(cached, listOf(1, 2, 3))
        val request = WatchlistSnapshotRequest(listOf(1, 3))
        val empty = WatchlistUpdated(3, 0, emptyList())
        `when`(client.snapshots("test-key", request)).thenReturn(listOf(recovered, empty))

        assertThat(resolver.resolve(listOf(1, 2, 3))).containsExactlyInAnyOrder(cached, recovered, empty)
        verify(client).snapshots("test-key", request)
        verifyNoMoreInteractions(client)
    }

    @Test
    fun `fallback failure still returns the cache hits`() {
        val cached = WatchlistUpdated(2, 14, listOf("005930"))
        batchWithMisses(cached, listOf(1, 2))
        `when`(client.snapshots("test-key", WatchlistSnapshotRequest(listOf(1))))
            .thenThrow(IllegalStateException("API unavailable"))

        assertThat(resolver.resolve(listOf(1, 2))).containsExactly(cached)
        assertThat(meters.counter("ws.watchlist.fallback.failures").count()).isEqualTo(1.0)
    }

    @Test
    fun `Redis connection failure does not fan out to core API`() {
        `when`(values.multiGet(listOf(Keys.watchlistSnapshot(1))))
            .thenThrow(RedisConnectionFailureException("Redis unavailable"))

        assertThrows<RedisConnectionFailureException> { resolver.resolve(listOf(1)) }
        verifyNoInteractions(client)
    }

    private fun batchWithMisses(cached: WatchlistUpdated, ids: List<Long>) {
        `when`(values.multiGet(ids.map(Keys::watchlistSnapshot)))
            .thenReturn(ids.map { if (it == cached.userId) mapper.writeValueAsString(cached) else null })
    }

    @Suppress("UNCHECKED_CAST")
    private fun redisValueOperations(): ValueOperations<String, String> {
        val ops = mock(ValueOperations::class.java) as ValueOperations<String, String>
        `when`(redis.opsForValue()).thenReturn(ops)
        return ops
    }
}
