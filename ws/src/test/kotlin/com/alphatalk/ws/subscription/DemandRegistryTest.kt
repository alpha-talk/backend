package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.ws.relay.ChannelSubscriber
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class DemandRegistryTest {
    private class FakeSubscriber : ChannelSubscriber {
        val active = linkedSetOf<String>()
        val subscribeCalls = mutableListOf<String>()
        val unsubscribeCalls = mutableListOf<String>()

        override fun subscribe(channel: String) {
            subscribeCalls += channel
            check(active.add(channel)) { "duplicate subscribe: $channel" }
        }

        override fun unsubscribe(channel: String) {
            unsubscribeCalls += channel
            check(active.remove(channel)) { "unsubscribe without subscribe: $channel" }
        }
    }

    private val subscriber = FakeSubscriber()
    private val syncTrigger = DemandSyncTrigger()
    private val meterRegistry = SimpleMeterRegistry()
    private val registry = DemandRegistry(subscriber, syncTrigger, DemandLockMetrics(meterRegistry))

    private fun syncRequested(): Boolean = syncTrigger.await(0)

    private fun connectAndAttach(sessionId: String, userId: Long, watchlist: Set<String>) {
        registry.registerSession(sessionId, userId)
        val target = requireNotNull(registry.startWatchlist(sessionId))
        registry.applyWatchlist(WatchlistUpdated(userId, 1, watchlist.toList()), target)
    }

    @Nested
    inner class Attendance {
        @Test
        fun `registerSession - 출석만 기록, Redis 구독 없음`() {
            registry.registerSession("s1", 1L)

            assertThat(subscriber.subscribeCalls).isEmpty()
            assertThat(registry.connectedSessionCount()).isEqualTo(1)
            assertThat(registry.isUserConnected(1L)).isTrue()
            assertThat(registry.watchlistTargets()).isEmpty()
        }

        @Test
        fun `needsWatchlist - 미등록 세션은 false, 부착 후 false`() {
            assertThat(registry.startWatchlist("ghost")).isNull()

            connectAndAttach("s1", 1L, setOf("005930"))
            assertThat(registry.startWatchlist("s1")).isNull()
        }

        @Test
        fun `같은 유저 두 번째 세션 - 유저 watchlist가 이미 있으면 needsWatchlist false`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            assertThat(registry.startWatchlist("s2")).isNull()
        }
    }

    @Nested
    inner class WatchlistAttach {
        @Test
        fun `부착 - 관심목록 각 code의 quote+stream 구독`() {
            connectAndAttach("s1", 1L, setOf("005930", "000660"))

            assertThat(subscriber.active).containsExactlyInAnyOrder(
                "quote:005930", "stream:005930", "quote:000660", "stream:000660",
            )
            assertThat(registry.usersWatching("005930")).containsExactly(1L)
        }

        @Test
        fun `죽은 세션에 부착 - 무시 (유령 세션 차단)`() {
            registry.registerSession("s1", 1L)
            val target = requireNotNull(registry.startWatchlist("s1"))
            registry.removeSession("s1")

            registry.applyWatchlist(WatchlistUpdated(1, 1, listOf("005930")), target)

            assertThat(subscriber.subscribeCalls).isEmpty()
            assertThat(registry.usersWatching("005930")).isEmpty()
        }

        @Test
        fun `같은 유저 중복 부착 - 두 번째는 무시`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            assertThat(registry.startWatchlist("s2")).isNull()

            assertThat(registry.usersWatching("999999")).isEmpty()
            assertThat(registry.usersWatching("005930")).containsExactly(1L)
        }

        @Test
        fun `두 유저가 같은 code - 채널은 1회만 구독, 한 명 나가도 유지`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            connectAndAttach("s2", 2L, setOf("005930"))

            assertThat(subscriber.subscribeCalls.filter { it == "quote:005930" }).hasSize(1)
            assertThat(registry.usersWatching("005930")).containsExactlyInAnyOrder(1L, 2L)

            registry.removeSession("s1")
            assertThat(subscriber.active).contains("quote:005930", "stream:005930")
            assertThat(registry.usersWatching("005930")).containsExactly(2L)
        }

        @Test
        fun `유저 마지막 세션 종료 - 수요 0이 된 채널 해지`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            registry.removeSession("s1")
            assertThat(subscriber.active).isNotEmpty

            registry.removeSession("s2")
            assertThat(subscriber.active).isEmpty()
            assertThat(registry.usersWatching("005930")).isEmpty()
            assertThat(registry.isUserConnected(1L)).isFalse()
        }

        @Test
        fun `미지 세션 removeSession - 무해`() {
            registry.removeSession("ghost")
            assertThat(subscriber.unsubscribeCalls).isEmpty()
        }
    }

    @Nested
    inner class WatchlistSnapshots {
        @Test
        fun `a full snapshot skips missing revisions and replaces the old list`() {
            connectAndAttach("s1", 1L, setOf("005930"))

            registry.applyWatchlist(WatchlistUpdated(1, 14, listOf("000660")))

            assertThat(subscriber.active).containsExactlyInAnyOrder("quote:000660", "stream:000660")
            assertThat(registry.usersWatching("005930")).isEmpty()
            assertThat(registry.usersWatching("000660")).containsExactly(1L)
        }

        @Test
        fun `events for inactive users do not create demand`() {
            registry.registerSession("s1", 1L)
            registry.applyWatchlist(WatchlistUpdated(1, 2, listOf("005930")))

            assertThat(subscriber.subscribeCalls).isEmpty()
        }

        @Test
        fun `an event wins over an older initial response and duplicate versions`() {
            registry.registerSession("s1", 1L)
            val target = requireNotNull(registry.startWatchlist("s1"))

            registry.applyWatchlist(WatchlistUpdated(1, 14, listOf("000660")))
            registry.applyWatchlist(WatchlistUpdated(1, 13, listOf("005930")), target)
            registry.applyWatchlist(WatchlistUpdated(1, 14, listOf("005930")))

            assertThat(subscriber.active).containsExactlyInAnyOrder("quote:000660", "stream:000660")
        }

        @Test
        fun `a response from a previous connection is discarded`() {
            registry.registerSession("s1", 1L)
            val previous = requireNotNull(registry.startWatchlist("s1"))
            registry.removeSession("s1")
            connectAndAttach("s2", 1L, setOf("005930"))

            registry.applyWatchlist(WatchlistUpdated(1, 9, listOf("000660")), previous)

            assertThat(registry.usersWatching("000660")).isEmpty()
            assertThat(registry.usersWatching("005930")).containsExactly(1L)
        }

        @Test
        fun `an empty snapshot preserves another user's demand`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            connectAndAttach("s2", 2L, setOf("005930"))

            registry.applyWatchlist(WatchlistUpdated(1, 2, emptyList()))

            assertThat(subscriber.active).contains("quote:005930")
            assertThat(registry.usersWatching("005930")).containsExactly(2L)
        }
    }

    @Nested
    inner class RoomDemand {
        @Test
        fun `방 첫 구독 - post 채널 구독, 마지막 해제 - 해지`() {
            registry.registerSession("s1", 1L)
            registry.registerSession("s2", 2L)

            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            registry.subscribeRoom("s2", "sub-1", ChannelKind.POST, "005930")
            assertThat(subscriber.subscribeCalls.filter { it == "post:005930" }).hasSize(1)

            registry.unsubscribeById("s1", "sub-1")
            assertThat(subscriber.active).contains("post:005930")

            registry.unsubscribeById("s2", "sub-1")
            assertThat(subscriber.active).doesNotContain("post:005930")
        }

        @Test
        fun `세션 종료 - 방 구독 자동 회수`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            registry.subscribeRoom("s1", "sub-2", ChannelKind.TRADE, "005930")

            registry.removeSession("s1")

            assertThat(subscriber.active).isEmpty()
        }

        @Test
        fun `같은 세션이 다른 subId로 같은 방 구독 - 하나만 해제해도 채널 유지`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            registry.subscribeRoom("s1", "sub-2", ChannelKind.POST, "005930")
            assertThat(subscriber.subscribeCalls.filter { it == "post:005930" }).hasSize(1)

            registry.unsubscribeById("s1", "sub-1")
            assertThat(subscriber.active).contains("post:005930")

            registry.unsubscribeById("s1", "sub-2")
            assertThat(subscriber.active).doesNotContain("post:005930")
        }

        @Test
        fun `같은 subId 재사용 - 이전 방 해제 후 새 방 구독`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")

            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "000660")

            assertThat(subscriber.active).contains("post:000660")
            assertThat(subscriber.active).doesNotContain("post:005930")
        }

        @Test
        fun `방 구독이 아닌 subId의 UNSUBSCRIBE - 무해 (user-queue 구독 해제 등)`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.unsubscribeById("s1", "sub-user-queue")

            assertThat(subscriber.active).contains("quote:005930")
        }

        @Test
        fun `watchlist code와 방 code가 겹쳐도 독립 관리`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")

            registry.unsubscribeById("s1", "sub-1")
            assertThat(subscriber.active).containsExactlyInAnyOrder("quote:005930", "stream:005930")
        }
    }

    @Nested
    inner class RoomQuoteDemand {
        @Test
        fun `방 quote 첫 구독 - quote 채널 구독, 마지막 해제 - 해지`() {
            registry.registerSession("s1", 1L)
            registry.registerSession("s2", 2L)

            registry.subscribeRoom("s1", "sub-1", ChannelKind.QUOTE, "005930")
            registry.subscribeRoom("s2", "sub-1", ChannelKind.QUOTE, "005930")
            assertThat(subscriber.subscribeCalls.filter { it == "quote:005930" }).hasSize(1)
            assertThat(registry.roomHasQuoteViewers("005930")).isTrue()

            registry.unsubscribeById("s1", "sub-1")
            assertThat(subscriber.active).contains("quote:005930")

            registry.unsubscribeById("s2", "sub-1")
            assertThat(subscriber.active).doesNotContain("quote:005930")
            assertThat(registry.roomHasQuoteViewers("005930")).isFalse()
        }

        @Test
        fun `관심목록에 이미 있는 code의 방 quote 구독 - 중복 구독 호출 없음`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 2L)

            registry.subscribeRoom("s2", "sub-1", ChannelKind.QUOTE, "005930")

            assertThat(subscriber.subscribeCalls.filter { it == "quote:005930" }).hasSize(1)
            assertThat(registry.roomHasQuoteViewers("005930")).isTrue()
        }

        @Test
        fun `관심목록이 비어도 방 quote 구독자가 남아 있으면 quote 채널 유지`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 2L)
            registry.subscribeRoom("s2", "sub-1", ChannelKind.QUOTE, "005930")

            registry.removeSession("s1")

            assertThat(subscriber.active).contains("quote:005930")
            assertThat(subscriber.active).doesNotContain("stream:005930")

            registry.unsubscribeById("s2", "sub-1")
            assertThat(subscriber.active).doesNotContain("quote:005930")
        }

        @Test
        fun `방 quote를 해제해도 관심목록 유저가 남아 있으면 quote 채널 유지`() {
            registry.registerSession("s2", 2L)
            registry.subscribeRoom("s2", "sub-1", ChannelKind.QUOTE, "005930")
            connectAndAttach("s1", 1L, setOf("005930"))

            registry.unsubscribeById("s2", "sub-1")

            assertThat(subscriber.active).contains("quote:005930", "stream:005930")
            assertThat(registry.roomHasQuoteViewers("005930")).isFalse()

            registry.removeSession("s1")
            assertThat(subscriber.active).isEmpty()
        }

        @Test
        fun `세션 종료 - 방 quote 구독 자동 회수`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.QUOTE, "005930")

            registry.removeSession("s1")

            assertThat(subscriber.active).isEmpty()
            assertThat(registry.roomHasQuoteViewers("005930")).isFalse()
        }

        @Test
        fun `방 quote 구독 - room 스냅샷에 계상`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.QUOTE, "005930")

            assertThat(registry.demandSnapshot().room).isEqualTo(mapOf("005930" to 1))
        }
    }

    @Nested
    inner class Snapshot {
        @Test
        fun `quote 스냅샷 - 유저 단위로 세고 마지막 유저가 나가면 코드가 사라진다`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            connectAndAttach("s2", 2L, setOf("005930"))
            assertThat(registry.demandSnapshot().quote).isEqualTo(mapOf("005930" to 2))

            registry.removeSession("s1")
            assertThat(registry.demandSnapshot().quote).isEqualTo(mapOf("005930" to 1))

            registry.removeSession("s2")
            assertThat(registry.demandSnapshot().quote).isEmpty()
        }

        @Test
        fun `같은 유저 세션 두 개 - quote는 유저당 1만 센다`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            assertThat(registry.demandSnapshot().quote).isEqualTo(mapOf("005930" to 1))

            registry.removeSession("s1")
            assertThat(registry.demandSnapshot().quote).isEqualTo(mapOf("005930" to 1))

            registry.removeSession("s2")
            assertThat(registry.demandSnapshot().quote).isEmpty()
        }

        @Test
        fun `room 스냅샷 - 구독 단위로 세고 kind가 달라도 code로 합산한다`() {
            registry.registerSession("s1", 1L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            registry.subscribeRoom("s1", "sub-2", ChannelKind.TRADE, "005930")
            assertThat(registry.demandSnapshot().room).isEqualTo(mapOf("005930" to 2))

            registry.unsubscribeById("s1", "sub-1")
            assertThat(registry.demandSnapshot().room).isEqualTo(mapOf("005930" to 1))

            registry.removeSession("s1")
            assertThat(registry.demandSnapshot().room).isEmpty()
        }

        @Test
        fun `watchlist diff - added·removed가 스냅샷에 반영된다`() {
            connectAndAttach("s1", 1L, setOf("005930"))

            registry.applyWatchlist(WatchlistUpdated(1, 2, listOf("000660")))

            assertThat(registry.demandSnapshot().quote).isEqualTo(mapOf("000660" to 1))
        }
    }

    @Nested
    inner class SyncTriggering {
        @Test
        fun `0↔1 전이에서만 동기화를 트리거한다 - quote`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            assertThat(syncRequested()).isTrue()

            connectAndAttach("s2", 2L, setOf("005930"))
            assertThat(syncRequested()).isFalse()

            registry.removeSession("s1")
            assertThat(syncRequested()).isFalse()

            registry.removeSession("s2")
            assertThat(syncRequested()).isTrue()
        }

        @Test
        fun `0↔1 전이에서만 동기화를 트리거한다 - room`() {
            registry.registerSession("s1", 1L)
            registry.registerSession("s2", 2L)
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            assertThat(syncRequested()).isTrue()

            registry.subscribeRoom("s2", "sub-1", ChannelKind.POST, "005930")
            assertThat(syncRequested()).isFalse()

            registry.unsubscribeById("s1", "sub-1")
            assertThat(syncRequested()).isFalse()

            registry.unsubscribeById("s2", "sub-1")
            assertThat(syncRequested()).isTrue()
        }

        @Test
        fun `트리거는 락을 잡지 않는 논블로킹 신호다 - 연속 요청이 코얼레싱된다`() {
            connectAndAttach("s1", 1L, setOf("005930", "000660", "035420"))

            assertThat(syncRequested()).isTrue()
            assertThat(syncRequested()).isFalse()
        }
    }
}
