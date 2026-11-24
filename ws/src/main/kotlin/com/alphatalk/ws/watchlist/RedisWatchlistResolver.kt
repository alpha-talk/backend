package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component
import java.util.concurrent.Semaphore

@Component
class RedisWatchlistResolver(
    private val redis: StringRedisTemplate,
    private val mapper: ObjectMapper,
    private val client: WatchlistSnapshotClient,
    @param:Value("\${ALPHATALK_WATCHLIST_INTERNAL_KEY:}") private val key: String,
    meters: MeterRegistry,
) : WatchlistResolver {
    private val log = LoggerFactory.getLogger(javaClass)
    private val hits = meters.counter("ws.watchlist.cache.hits")
    private val misses = meters.counter("ws.watchlist.cache.misses")
    private val invalid = meters.counter("ws.watchlist.cache.invalid")
    private val fallbackFailures = meters.counter("ws.watchlist.fallback.failures")
    private val fallbackDeferred = meters.counter("ws.watchlist.fallback.deferred")
    private val fallbackSlots = Semaphore(2)

    override fun resolve(userIds: List<Long>): List<WatchlistUpdated> {
        if (userIds.isEmpty()) return emptyList()
        val values = requireNotNull(redis.opsForValue().multiGet(userIds.map(Keys::watchlistSnapshot)))
        check(values.size == userIds.size)
        val snapshots = mutableListOf<WatchlistUpdated>()
        val missing = mutableListOf<Long>()
        userIds.zip(values).forEach { (userId, value) ->
            if (value == null) {
                missing += userId
            } else {
                decode(userId, value)?.let(snapshots::add)
            }
        }
        hits.increment(snapshots.size.toDouble())
        misses.increment(missing.size.toDouble())
        if (missing.isNotEmpty()) snapshots += resolveMissing(missing)
        return snapshots
    }

    private fun decode(userId: Long, value: String): WatchlistUpdated? {
        val snapshot = try {
            mapper.readValue(value, WatchlistUpdated::class.java)
        } catch (_: JsonProcessingException) {
            null
        }
        if (snapshot?.userId == userId && snapshot.rev >= 0) return snapshot
        invalid.increment()
        return null
    }

    private fun resolveMissing(userIds: List<Long>): List<WatchlistUpdated> {
        if (!fallbackSlots.tryAcquire()) {
            fallbackDeferred.increment(userIds.size.toDouble())
            return emptyList()
        }
        try {
            check(key.isNotBlank()) { "Watchlist internal key is missing" }
            val snapshots = requireNotNull(client.snapshots(key, WatchlistSnapshotRequest(userIds)))
            check(snapshots.size == userIds.size && snapshots.map { it.userId }.toSet() == userIds.toSet()) {
                "Watchlist fallback response does not match request"
            }
            return snapshots
        } catch (e: Exception) {
            fallbackFailures.increment()
            log.warn("watchlist fallback failed. users={}", userIds.size, e)
            return emptyList()
        } finally {
            fallbackSlots.release()
        }
    }
}
