package com.alphatalk.ws.client

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Destinations
import org.springframework.messaging.simp.SimpMessagingTemplate
import org.springframework.stereotype.Component

@Component
class BrokerMessageSink(private val template: SimpMessagingTemplate) : ClientMessageSink {
    override fun sendToUser(userId: Long, kind: ChannelKind, payload: Any) {
        val destination = when (kind) {
            ChannelKind.QUOTE -> Destinations.QUEUE_QUOTE
            ChannelKind.STREAM -> Destinations.QUEUE_STREAM
            else -> throw IllegalArgumentException("not a user-queue kind: $kind")
        }
        template.convertAndSendToUser(userId.toString(), destination, payload)
    }

    override fun sendToRoom(kind: ChannelKind, code: String, payload: Any) {
        val destination = when (kind) {
            ChannelKind.POST -> Destinations.roomPosts(code)
            ChannelKind.TRADE -> Destinations.roomTrade(code)
            ChannelKind.DEPTH -> Destinations.roomDepth(code)
            else -> throw IllegalArgumentException("not a room kind: $kind")
        }
        template.convertAndSend(destination, payload)
    }
}
