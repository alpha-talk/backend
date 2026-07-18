package com.alphatalk.ws.relay

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.ws.client.ClientMessageSink
import com.alphatalk.ws.subscription.DemandQuery
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

abstract class EnvelopeRelayHandler(
    private val objectMapper: ObjectMapper,
    meterRegistry: MeterRegistry,
) : RedisChannelHandler {
    private val log = LoggerFactory.getLogger(javaClass)
    private val dropped = meterRegistry.counter("ws.relay.envelope.dropped", "kind", javaClass.simpleName)

    protected fun parseOrDrop(payload: ByteArray): JsonNode? = try {
        objectMapper.readTree(payload)
    } catch (e: Exception) {
        dropped.increment()
        log.warn("envelope parse failed, dropped. kind={}", kind)
        null
    }
}

abstract class WatchlistFanoutHandler(
    final override val kind: ChannelKind,
    private val demand: DemandQuery,
    private val sink: ClientMessageSink,
    objectMapper: ObjectMapper,
    meterRegistry: MeterRegistry,
) : EnvelopeRelayHandler(objectMapper, meterRegistry) {
    final override fun handle(code: String?, payload: ByteArray) {
        if (code == null) return
        val envelope = parseOrDrop(payload) ?: return
        for (userId in demand.usersWatching(code)) {
            sink.sendToUser(userId, kind, envelope)
        }
    }
}

abstract class RoomFanoutHandler(
    final override val kind: ChannelKind,
    private val sink: ClientMessageSink,
    objectMapper: ObjectMapper,
    meterRegistry: MeterRegistry,
) : EnvelopeRelayHandler(objectMapper, meterRegistry) {
    final override fun handle(code: String?, payload: ByteArray) {
        if (code == null) return
        val envelope = parseOrDrop(payload) ?: return
        sink.sendToRoom(kind, code, envelope)
    }
}

@Component
class QuoteRelayHandler(demand: DemandQuery, sink: ClientMessageSink, om: ObjectMapper, mr: MeterRegistry) :
    WatchlistFanoutHandler(ChannelKind.QUOTE, demand, sink, om, mr)

@Component
class StreamRelayHandler(demand: DemandQuery, sink: ClientMessageSink, om: ObjectMapper, mr: MeterRegistry) :
    WatchlistFanoutHandler(ChannelKind.STREAM, demand, sink, om, mr)

@Component
class PostRelayHandler(sink: ClientMessageSink, om: ObjectMapper, mr: MeterRegistry) :
    RoomFanoutHandler(ChannelKind.POST, sink, om, mr)

@Component
class TradeRelayHandler(sink: ClientMessageSink, om: ObjectMapper, mr: MeterRegistry) :
    RoomFanoutHandler(ChannelKind.TRADE, sink, om, mr)

@Component
class DepthRelayHandler(sink: ClientMessageSink, om: ObjectMapper, mr: MeterRegistry) :
    RoomFanoutHandler(ChannelKind.DEPTH, sink, om, mr)
