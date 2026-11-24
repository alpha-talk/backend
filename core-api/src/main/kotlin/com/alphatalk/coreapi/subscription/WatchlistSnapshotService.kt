package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.envelope.WatchlistUpdated
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class WatchlistSnapshotService(
    private val reader: WatchlistSnapshotReader,
    private val redis: RedisWatchlistSnapshots,
    meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val fillFailures = meters.counter("core.watchlist.cache.fill.failures")

    fun readAndFill(userIds: List<Long>): List<WatchlistUpdated> {
        val snapshots = reader.read(userIds)
        runCatching { redis.fillMissing(snapshots) }.onFailure {
            fillFailures.increment()
            log.warn("watchlist cache fill failed. users={}", userIds.size, it)
        }
        return snapshots
    }
}
