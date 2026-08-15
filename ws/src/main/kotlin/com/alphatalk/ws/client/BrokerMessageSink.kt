package com.alphatalk.ws.client

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Destinations
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component

@Component
class BrokerMessageSink(
    private val template: SimpMessagingTemplate,
    meterRegistry: MeterRegistry,
) : ClientMessageSink {
    private val sentToUser = meterRegistry.counter("ws.relay.sent", "target", "user")
    private val sentToRoom = meterRegistry.counter("ws.relay.sent", "target", "room")

    override fun sendToUser(userId: Long, kind: ChannelKind, payload: Any) {
        val destination = when (kind) {
            ChannelKind.QUOTE -> Destinations.QUEUE_QUOTE
            ChannelKind.STREAM -> Destinations.QUEUE_STREAM
            else -> throw IllegalArgumentException("not a user-queue kind: $kind")
        }
        template.convertAndSendToUser(userId.toString(), destination, payload)
        sentToUser.increment()
    }

    override fun sendToRoom(kind: ChannelKind, code: String, payload: Any) {
        val destination = when (kind) {
            ChannelKind.QUOTE -> Destinations.roomQuote(code)
            ChannelKind.POST -> Destinations.roomPosts(code)
            ChannelKind.TRADE -> Destinations.roomTrade(code)
            ChannelKind.DEPTH -> Destinations.roomDepth(code)
            else -> throw IllegalArgumentException("not a room kind: $kind")
        }
        template.convertAndSend(destination, payload)
        sentToRoom.increment()
    }
}
