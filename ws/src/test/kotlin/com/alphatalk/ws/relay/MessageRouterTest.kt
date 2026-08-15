package com.alphatalk.ws.relay

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.ws.client.ClientMessageSink
import com.alphatalk.ws.subscription.DemandMutator
import com.alphatalk.ws.subscription.DemandQuery
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.DefaultMessage

class MessageRouterTest {
    private val objectMapper = jacksonObjectMapper()
    private val meterRegistry = SimpleMeterRegistry()

    private class RecordingSink : ClientMessageSink {
        val userSends = mutableListOf<Triple<Long, ChannelKind, Any>>()
        val roomSends = mutableListOf<Triple<ChannelKind, String, Any>>()

        override fun sendToUser(userId: Long, kind: ChannelKind, payload: Any) {
            userSends += Triple(userId, kind, payload)
        }

        override fun sendToRoom(kind: ChannelKind, code: String, payload: Any) {
            roomSends += Triple(kind, code, payload)
        }
    }

    private class FakeDemand(
        private val watchers: Map<String, Set<Long>>,
        private val roomQuoteCodes: Set<String> = emptySet(),
    ) : DemandQuery {
        override fun usersWatching(code: String) = watchers[code] ?: emptySet()
        override fun roomHasQuoteViewers(code: String) = code in roomQuoteCodes
        override fun isUserConnected(userId: Long) = watchers.values.any { userId in it }
        override fun needsWatchlist(sessionId: String) = false
        override fun connectedUserIds() = watchers.values.flatten().toSet()
        override fun connectedSessionCount() = 0
    }

    private class RecordingMutator : DemandMutator {
        val diffs = mutableListOf<Triple<Long, Collection<String>, Collection<String>>>()
        override fun registerSession(sessionId: String, userId: Long) = Unit
        override fun attachWatchlist(sessionId: String, watchlist: Set<String>) = Unit
        override fun removeSession(sessionId: String) = Unit
        override fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String) = Unit
        override fun unsubscribeById(sessionId: String, subscriptionId: String) = Unit
        override fun applyWatchlistDiff(userId: Long, added: Collection<String>, removed: Collection<String>) {
            diffs += Triple(userId, added, removed)
        }
    }

    private val sink = RecordingSink()
    private val demand = FakeDemand(mapOf("005930" to setOf(1L, 2L)))
    private val mutator = RecordingMutator()

    private fun router(demand: DemandQuery = this.demand): MessageRouter {
        val handlers = listOf(
            QuoteRelayHandler(demand, sink, objectMapper, meterRegistry),
            StreamRelayHandler(demand, sink, objectMapper, meterRegistry),
            PostRelayHandler(sink, objectMapper, meterRegistry),
            WatchlistUpdateHandler(mutator, objectMapper, meterRegistry),
        )
        return MessageRouter(handlers, meterRegistry)
    }

    private fun redisMessage(channel: String, body: String) =
        DefaultMessage(channel.toByteArray(), body.toByteArray())

    @Test
    fun `quote - 그 code를 보는 유저 전원에게 fan-out`() {
        val body = """{"type":"quote","code":"005930","ts":1,"data":{"price":71200}}"""
        router().onMessage(redisMessage("quote:005930", body), null)

        assertThat(sink.userSends).hasSize(2)
        assertThat(sink.userSends.map { it.first }).containsExactlyInAnyOrder(1L, 2L)
        assertThat(sink.userSends).allSatisfy { (_, kind, payload) ->
            assertThat(kind).isEqualTo(ChannelKind.QUOTE)
            assertThat((payload as JsonNode)["data"]["price"].asLong()).isEqualTo(71200)
        }
    }

    @Test
    fun `보는 유저 없는 code - 발행 없음`() {
        router().onMessage(redisMessage("quote:999999", """{"type":"quote"}"""), null)
        assertThat(sink.userSends).isEmpty()
        assertThat(sink.roomSends).isEmpty()
    }

    @Test
    fun `quote - 방 quote 구독자가 있으면 방 토픽으로도 1회 발행`() {
        val withRoom = FakeDemand(mapOf("005930" to setOf(1L)), roomQuoteCodes = setOf("005930"))
        val body = """{"type":"quote","code":"005930","ts":1,"data":{"price":71200}}"""

        router(withRoom).onMessage(redisMessage("quote:005930", body), null)

        assertThat(sink.userSends).hasSize(1)
        assertThat(sink.roomSends).hasSize(1)
        val (kind, code, payload) = sink.roomSends.single()
        assertThat(kind).isEqualTo(ChannelKind.QUOTE)
        assertThat(code).isEqualTo("005930")
        assertThat((payload as JsonNode)["data"]["price"].asLong()).isEqualTo(71200)
    }

    @Test
    fun `quote - 관심목록 유저 없이 방 구독자만 있어도 방 토픽 발행`() {
        val roomOnly = FakeDemand(emptyMap(), roomQuoteCodes = setOf("005930"))
        val body = """{"type":"quote","code":"005930","ts":1,"data":{"price":71200}}"""

        router(roomOnly).onMessage(redisMessage("quote:005930", body), null)

        assertThat(sink.userSends).isEmpty()
        assertThat(sink.roomSends).hasSize(1)
    }

    @Test
    fun `post - 방 토픽으로 1회 발행`() {
        val body = """{"type":"post","code":"005930","eventId":"01J","ts":1,"data":{"kind":"post"}}"""
        router().onMessage(redisMessage("post:005930", body), null)

        assertThat(sink.roomSends).hasSize(1)
        val (kind, code, _) = sink.roomSends.single()
        assertThat(kind).isEqualTo(ChannelKind.POST)
        assertThat(code).isEqualTo("005930")
    }

    @Test
    fun `watchlist updated - diff 적용 위임`() {
        val body = """{"userId":1,"added":["000660"],"removed":["005930"],"ts":1}"""
        router().onMessage(redisMessage("watchlist:updated", body), null)

        assertThat(mutator.diffs).containsExactly(Triple(1L, listOf("000660"), listOf("005930")))
    }

    @Test
    fun `깨진 JSON - 드랍하고 relay는 계속`() {
        router().onMessage(redisMessage("quote:005930", "not-json{{{"), null)
        assertThat(sink.userSends).isEmpty()
        assertThat(meterRegistry.counter("ws.relay.envelope.dropped", "kind", "QuoteRelayHandler").count())
            .isEqualTo(1.0)

        router().onMessage(
            redisMessage("quote:005930", """{"type":"quote","code":"005930","ts":1,"data":{}}"""),
            null,
        )
        assertThat(sink.userSends).hasSize(2)
    }

    @Test
    fun `알 수 없는 채널 - unroutable 카운트 후 무시`() {
        router().onMessage(redisMessage("unknown:zzz", "{}"), null)
        assertThat(sink.userSends).isEmpty()
        assertThat(sink.roomSends).isEmpty()
        assertThat(meterRegistry.counter("ws.relay.unroutable").count()).isEqualTo(1.0)
    }

    @Test
    fun `핸들러 없는 종류 (trade 미등록) - unroutable 처리`() {
        router().onMessage(redisMessage("trade:005930", "{}"), null)
        assertThat(meterRegistry.counter("ws.relay.unroutable").count()).isEqualTo(1.0)
    }
}
