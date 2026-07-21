package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.ws.relay.ChannelSubscriber
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
    private val registry = DemandRegistry(subscriber)

    private fun connectAndAttach(sessionId: String, userId: Long, watchlist: Set<String>) {
        registry.registerSession(sessionId, userId)
        registry.attachWatchlist(sessionId, watchlist)
    }

    @Nested
    inner class Attendance {
        @Test
        fun `registerSession - 출석만 기록, Redis 구독 없음`() {
            registry.registerSession("s1", 1L)

            assertThat(subscriber.subscribeCalls).isEmpty()
            assertThat(registry.connectedSessionCount()).isEqualTo(1)
            assertThat(registry.isUserConnected(1L)).isTrue()
            assertThat(registry.needsWatchlist("s1")).isTrue()
        }

        @Test
        fun `needsWatchlist - 미등록 세션은 false, 부착 후 false`() {
            assertThat(registry.needsWatchlist("ghost")).isFalse()

            connectAndAttach("s1", 1L, setOf("005930"))
            assertThat(registry.needsWatchlist("s1")).isFalse()
        }

        @Test
        fun `같은 유저 두 번째 세션 - 유저 watchlist가 이미 있으면 needsWatchlist false`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            assertThat(registry.needsWatchlist("s2")).isFalse()
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
            registry.removeSession("s1")

            registry.attachWatchlist("s1", setOf("005930"))

            assertThat(subscriber.subscribeCalls).isEmpty()
            assertThat(registry.usersWatching("005930")).isEmpty()
        }

        @Test
        fun `같은 유저 중복 부착 - 두 번째는 무시`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L)

            registry.attachWatchlist("s2", setOf("999999"))

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
    inner class WatchlistDiff {
        @Test
        fun `added - 새 code 구독, removed - 수요 0이면 해지`() {
            connectAndAttach("s1", 1L, setOf("005930"))

            registry.applyWatchlistDiff(1L, added = listOf("000660"), removed = listOf("005930"))

            assertThat(subscriber.active).containsExactlyInAnyOrder("quote:000660", "stream:000660")
            assertThat(registry.usersWatching("005930")).isEmpty()
            assertThat(registry.usersWatching("000660")).containsExactly(1L)
        }

        @Test
        fun `접속하지 않은 유저의 diff - 무시 (broadcast-and-filter)`() {
            registry.applyWatchlistDiff(99L, added = listOf("005930"), removed = emptyList())

            assertThat(subscriber.subscribeCalls).isEmpty()
            assertThat(registry.usersWatching("005930")).isEmpty()
        }

        @Test
        fun `부착 전 도착한 diff - 버퍼링 후 부착 시 병합 (구버전 해소와의 레이스 방지)`() {
            registry.registerSession("s1", 1L)

            registry.applyWatchlistDiff(1L, added = listOf("000660"), removed = listOf("005930"))
            assertThat(subscriber.subscribeCalls).isEmpty()

            registry.attachWatchlist("s1", setOf("005930", "035420"))

            assertThat(registry.usersWatching("005930")).isEmpty()
            assertThat(registry.usersWatching("000660")).containsExactly(1L)
            assertThat(registry.usersWatching("035420")).containsExactly(1L)
        }

        @Test
        fun `부착 전 여러 diff - 종목별 최종 연산으로 압축 (추가 후 제거)`() {
            registry.registerSession("s1", 1L)

            registry.applyWatchlistDiff(1L, added = listOf("000660"), removed = emptyList())
            registry.applyWatchlistDiff(1L, added = emptyList(), removed = listOf("000660"))

            registry.attachWatchlist("s1", emptySet())

            assertThat(registry.usersWatching("000660")).isEmpty()
            assertThat(subscriber.subscribeCalls).isEmpty()
        }

        @Test
        fun `부착 전 여러 diff - 제거 후 재추가는 살아남는다`() {
            registry.registerSession("s1", 1L)

            registry.applyWatchlistDiff(1L, added = emptyList(), removed = listOf("000660"))
            registry.applyWatchlistDiff(1L, added = listOf("000660"), removed = emptyList())

            registry.attachWatchlist("s1", emptySet())

            assertThat(registry.usersWatching("000660")).containsExactly(1L)
        }

        @Test
        fun `부착 전 diff 후 마지막 세션 종료 - pending 잔류 없음`() {
            registry.registerSession("s1", 1L)
            registry.applyWatchlistDiff(1L, added = listOf("000660"), removed = emptyList())
            registry.removeSession("s1")

            registry.registerSession("s2", 1L)
            registry.attachWatchlist("s2", setOf("005930"))

            assertThat(registry.usersWatching("000660")).isEmpty()
            assertThat(registry.usersWatching("005930")).containsExactly(1L)
        }

        @Test
        fun `중복 added - 멱등`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            val calls = subscriber.subscribeCalls.size

            registry.applyWatchlistDiff(1L, added = listOf("005930"), removed = emptyList())

            assertThat(subscriber.subscribeCalls).hasSize(calls)
        }

        @Test
        fun `다른 유저도 보는 code의 removed - 채널 유지`() {
            connectAndAttach("s1", 1L, setOf("005930"))
            connectAndAttach("s2", 2L, setOf("005930"))

            registry.applyWatchlistDiff(1L, added = emptyList(), removed = listOf("005930"))

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
}
