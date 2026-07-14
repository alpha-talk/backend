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

    @Nested
    inner class WatchlistDemand {
        @Test
        fun `유저 첫 세션 접속 - 관심목록 각 code의 quote+stream 구독`() {
            registry.registerSession("s1", 1L, setOf("005930", "000660"))

            assertThat(subscriber.active).containsExactlyInAnyOrder(
                "quote:005930", "stream:005930", "quote:000660", "stream:000660",
            )
            assertThat(registry.usersWatching("005930")).containsExactly(1L)
        }

        @Test
        fun `같은 유저 두 번째 세션 - 새 구독 없음`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            val callsAfterFirst = subscriber.subscribeCalls.size

            registry.registerSession("s2", 1L, setOf("005930"))

            assertThat(subscriber.subscribeCalls).hasSize(callsAfterFirst)
        }

        @Test
        fun `두 유저가 같은 code - 채널은 1회만 구독, 한 명 나가도 유지`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 2L, setOf("005930"))

            assertThat(subscriber.subscribeCalls.filter { it == "quote:005930" }).hasSize(1)
            assertThat(registry.usersWatching("005930")).containsExactlyInAnyOrder(1L, 2L)

            registry.removeSession("s1")
            assertThat(subscriber.active).contains("quote:005930", "stream:005930")
            assertThat(registry.usersWatching("005930")).containsExactly(2L)
        }

        @Test
        fun `유저 마지막 세션 종료 - 수요 0이 된 채널 해지`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 1L, setOf("005930"))

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
            registry.registerSession("s1", 1L, setOf("005930"))

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
        fun `중복 added - 멱등`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            val calls = subscriber.subscribeCalls.size

            registry.applyWatchlistDiff(1L, added = listOf("005930"), removed = emptyList())

            assertThat(subscriber.subscribeCalls).hasSize(calls)
        }

        @Test
        fun `다른 유저도 보는 code의 removed - 채널 유지`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            registry.registerSession("s2", 2L, setOf("005930"))

            registry.applyWatchlistDiff(1L, added = emptyList(), removed = listOf("005930"))

            assertThat(subscriber.active).contains("quote:005930")
            assertThat(registry.usersWatching("005930")).containsExactly(2L)
        }
    }

    @Nested
    inner class RoomDemand {
        @Test
        fun `방 첫 구독 - post 채널 구독, 마지막 해제 - 해지`() {
            registry.registerSession("s1", 1L, emptySet())
            registry.registerSession("s2", 2L, emptySet())

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
            registry.registerSession("s1", 1L, emptySet())
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")
            registry.subscribeRoom("s1", "sub-2", ChannelKind.TRADE, "005930")

            registry.removeSession("s1")

            assertThat(subscriber.active).isEmpty()
        }

        @Test
        fun `방 구독이 아닌 subId의 UNSUBSCRIBE - 무해 (user-queue 구독 해제 등)`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            registry.unsubscribeById("s1", "sub-user-queue")

            assertThat(subscriber.active).contains("quote:005930")
        }

        @Test
        fun `watchlist code와 방 code가 겹쳐도 독립 관리`() {
            registry.registerSession("s1", 1L, setOf("005930"))
            registry.subscribeRoom("s1", "sub-1", ChannelKind.POST, "005930")

            registry.unsubscribeById("s1", "sub-1")
            assertThat(subscriber.active).containsExactlyInAnyOrder("quote:005930", "stream:005930")
        }
    }
}
