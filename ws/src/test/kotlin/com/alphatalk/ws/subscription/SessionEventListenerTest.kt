package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Destinations
import com.alphatalk.ws.auth.StompPrincipal
import com.alphatalk.ws.presence.PresenceRegistry
import com.alphatalk.ws.watchlist.WatchlistResolver
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.messaging.Message
import org.springframework.messaging.simp.stomp.StompCommand
import org.springframework.messaging.simp.stomp.StompHeaderAccessor
import org.springframework.messaging.support.MessageBuilder
import org.springframework.web.socket.messaging.SessionConnectedEvent
import org.springframework.web.socket.messaging.SessionSubscribeEvent

class SessionEventListenerTest {

    private class RecordingDemand : DemandQuery, DemandMutator {
        val registerCalls = mutableListOf<Triple<String, Long, Set<String>>>()
        val roomCalls = mutableListOf<Pair<String, ChannelKind>>()
        private val registered = mutableSetOf<String>()

        override fun registerSession(sessionId: String, userId: Long, watchlist: Set<String>) {
            registerCalls += Triple(sessionId, userId, watchlist)
            registered += sessionId
        }

        override fun isSessionRegistered(sessionId: String) = sessionId in registered
        override fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String) {
            roomCalls += sessionId to kind
        }

        override fun removeSession(sessionId: String) = Unit
        override fun unsubscribeById(sessionId: String, subscriptionId: String) = Unit
        override fun applyWatchlistDiff(userId: Long, added: Collection<String>, removed: Collection<String>) = Unit
        override fun usersWatching(code: String) = emptySet<Long>()
        override fun isUserConnected(userId: Long) = false
        override fun connectedUserIds() = emptySet<Long>()
        override fun connectedSessionCount() = 0
    }

    private class RecordingPresence : PresenceRegistry {
        val added = mutableListOf<Pair<Long, String>>()
        override fun add(userId: Long, sessionId: String) {
            added += userId to sessionId
        }

        override fun remove(userId: Long, sessionId: String) = Unit
        override fun refresh(userIds: Collection<Long>) = Unit
    }

    private class CountingResolver(private val delegate: WatchlistResolver) : WatchlistResolver {
        var invocations = 0
        override fun resolve(userId: Long): Set<String> {
            invocations++
            return delegate.resolve(userId)
        }
    }

    private val demand = RecordingDemand()
    private val presence = RecordingPresence()

    private fun listener(resolver: WatchlistResolver) =
        SessionEventListener(demand, demand, resolver, presence)

    private fun stompMessage(command: StompCommand, sessionId: String, destination: String? = null): Message<ByteArray> {
        val accessor = StompHeaderAccessor.create(command)
        accessor.sessionId = sessionId
        destination?.let {
            accessor.destination = it
            accessor.subscriptionId = "sub-1"
        }
        return MessageBuilder.createMessage(ByteArray(0), accessor.messageHeaders)
    }

    private fun connectedEvent(sessionId: String, userId: Long) = SessionConnectedEvent(
        this, stompMessage(StompCommand.CONNECTED, sessionId), StompPrincipal(userId),
    )

    private fun subscribeEvent(sessionId: String, userId: Long, destination: String) = SessionSubscribeEvent(
        this, stompMessage(StompCommand.SUBSCRIBE, sessionId, destination), StompPrincipal(userId),
    )

    @Test
    fun `onConnected - registerSession을 호출하지 않는다 (프레즌스만)`() {
        val resolver = CountingResolver { setOf("005930") }

        listener(resolver).onConnected(connectedEvent("s1", 1L))

        assertThat(demand.registerCalls).isEmpty()
        assertThat(resolver.invocations).isZero()
        assertThat(presence.added).containsExactly(1L to "s1")
    }

    @Test
    fun `첫 SUBSCRIBE - resolve 1회 + registerSession 1회`() {
        val resolver = CountingResolver { setOf("005930", "000660") }

        listener(resolver).onSubscribe(subscribeEvent("s1", 1L, Destinations.USER_QUEUE_QUOTE))

        assertThat(resolver.invocations).isEqualTo(1)
        assertThat(demand.registerCalls).containsExactly(Triple("s1", 1L, setOf("005930", "000660")))
    }

    @Test
    fun `같은 세션 두 번째 SUBSCRIBE - resolve와 registerSession 추가 호출 없음`() {
        val resolver = CountingResolver { setOf("005930") }
        val target = listener(resolver)

        target.onSubscribe(subscribeEvent("s1", 1L, Destinations.USER_QUEUE_QUOTE))
        target.onSubscribe(subscribeEvent("s1", 1L, Destinations.USER_QUEUE_STREAM))

        assertThat(resolver.invocations).isEqualTo(1)
        assertThat(demand.registerCalls).hasSize(1)
    }

    @Test
    fun `다른 세션의 SUBSCRIBE - 각자 등록`() {
        val resolver = CountingResolver { setOf("005930") }
        val target = listener(resolver)

        target.onSubscribe(subscribeEvent("s1", 1L, Destinations.USER_QUEUE_QUOTE))
        target.onSubscribe(subscribeEvent("s2", 2L, Destinations.USER_QUEUE_QUOTE))

        assertThat(resolver.invocations).isEqualTo(2)
        assertThat(demand.registerCalls.map { it.first }).containsExactly("s1", "s2")
    }

    @Test
    fun `resolve 실패 - 빈 관심목록으로 등록하고 세션은 계속`() {
        val resolver = CountingResolver { throw IllegalStateException("redis down") }

        listener(resolver).onSubscribe(subscribeEvent("s1", 1L, Destinations.USER_QUEUE_QUOTE))

        assertThat(demand.registerCalls).containsExactly(Triple("s1", 1L, emptySet()))
    }

    @Test
    fun `방 토픽이 첫 SUBSCRIBE여도 - 등록 후 방 구독까지 이어진다`() {
        val resolver = CountingResolver { setOf("005930") }

        listener(resolver).onSubscribe(subscribeEvent("s1", 1L, Destinations.roomPosts("000660")))

        assertThat(demand.registerCalls).hasSize(1)
        assertThat(demand.roomCalls).containsExactly("s1" to ChannelKind.POST)
    }
}
