package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.contracts.envelope.WatchlistUpdated
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

fun interface WatchlistBroadcaster {
    fun publish(snapshot: WatchlistUpdated)
}

@Component
class RedisWatchlistBroadcaster(
    private val redis: RedisWatchlistSnapshots,
    private val revisions: WatchlistRevisionJpaRepository,
    private val reader: WatchlistSnapshotReader,
    meters: MeterRegistry,
) : WatchlistBroadcaster {
    private val log = LoggerFactory.getLogger(javaClass)
    private val failures = meters.counter("core.watchlist.publish.failures")

    override fun publish(snapshot: WatchlistUpdated) {
        deliver(snapshot)
    }

    @Scheduled(fixedDelay = 5_000, initialDelay = 5_000)
    fun retryPending() {
        val pending = revisions.findRedisPendingUserIds(PageRequest.of(0, WatchlistSnapshots.MAX_BATCH_SIZE))
        if (pending.isEmpty()) return
        for (snapshot in reader.read(pending)) {
            if (!deliver(snapshot)) return
        }
    }

    private fun deliver(snapshot: WatchlistUpdated): Boolean {
        try {
            redis.publish(snapshot)
            revisions.clearRedisPending(snapshot.userId, snapshot.rev)
            return true
        } catch (e: Exception) {
            failures.increment()
            log.warn("watchlist publish failed. userId={} rev={}", snapshot.userId, snapshot.rev, e)
            return false
        }
    }
}
