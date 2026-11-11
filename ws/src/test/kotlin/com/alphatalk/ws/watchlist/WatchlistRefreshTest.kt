package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.relay.ChannelSubscriber
import com.alphatalk.ws.subscription.DemandRegistry
import com.alphatalk.ws.subscription.DemandSyncTrigger
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class WatchlistRefreshTest {
    private val registry = DemandRegistry(object : ChannelSubscriber {
        override fun subscribe(channel: String) = Unit
        override fun unsubscribe(channel: String) = Unit
    }, DemandSyncTrigger())

    @Test
    fun `reconciliation batches active users and repairs the missed final event`() {
        val requests = mutableListOf<List<Long>>()
        val refresh = WatchlistRefresh(
            registry,
            registry,
            { ids ->
                requests += ids
                ids.map { WatchlistUpdated(it, 14, listOf("000660")) }
            },
            { it.run() },
            SimpleMeterRegistry(),
        )
        (1L..205L).forEach { userId ->
            registry.registerSession("s$userId", userId)
            registry.startWatchlist("s$userId")
            registry.applyWatchlist(WatchlistUpdated(userId, 13, listOf("005930")))
        }
        registry.registerSession("idle", 206L)

        refresh.reconcile()

        assertThat(requests.map { it.size }).containsExactly(100, 100, 5)
        assertThat(requests.flatten()).doesNotHaveDuplicates().doesNotContain(206L)
        assertThat(registry.usersWatching("005930")).isEmpty()
        assertThat(registry.usersWatching("000660")).hasSize(205)
    }

    @Test
    fun `failed initial lookup is recovered without a new STOMP session`() {
        var ready = false
        val refresh = WatchlistRefresh(
            registry,
            registry,
            { ids ->
                check(ready)
                ids.map { WatchlistUpdated(it, 0, listOf("005930")) }
            },
            { it.run() },
            SimpleMeterRegistry(),
        )
        registry.registerSession("s1", 1L)
        val target = requireNotNull(registry.startWatchlist("s1"))

        refresh.loadInitial(target)
        assertThat(registry.usersWatching("005930")).isEmpty()

        ready = true
        refresh.reconcile()

        assertThat(registry.usersWatching("005930")).containsExactly(1L)
    }
}
