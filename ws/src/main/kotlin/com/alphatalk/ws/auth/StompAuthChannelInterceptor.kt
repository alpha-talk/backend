package com.alphatalk.ws.auth

import com.alphatalk.auth.InvalidTokenException
import com.alphatalk.auth.TokenVerifier
import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Destinations
import com.alphatalk.ws.config.WsProperties
import org.slf4j.LoggerFactory
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.MessagingException
import org.springframework.messaging.simp.stomp.StompCommand
import org.springframework.messaging.simp.stomp.StompHeaderAccessor
import org.springframework.messaging.support.ChannelInterceptor
import org.springframework.messaging.support.MessageHeaderAccessor
import org.springframework.stereotype.Component

@Component
class StompAuthChannelInterceptor(
    private val tokenVerifier: TokenVerifier,
    private val props: WsProperties,
) : ChannelInterceptor {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun preSend(message: Message<*>, channel: MessageChannel): Message<*> {
        val accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor::class.java)
            ?: return message
        when (accessor.command) {
            StompCommand.CONNECT -> authenticate(accessor)
            StompCommand.SUBSCRIBE -> authorizeSubscribe(accessor)
            StompCommand.SEND -> throw MessagingException("send-not-allowed")
            else -> Unit
        }
        return message
    }

    private fun authenticate(accessor: StompHeaderAccessor) {
        val header = accessor.getFirstNativeHeader(AUTHORIZATION)
            ?: throw MessagingException("unauthorized")
        if (!header.startsWith(BEARER_PREFIX)) throw MessagingException("unauthorized")
        val userId = try {
            tokenVerifier.verify(header.removePrefix(BEARER_PREFIX).trim())
        } catch (e: InvalidTokenException) {
            log.info("CONNECT rejected: invalid token ({})", e.message)
            throw MessagingException("unauthorized")
        }
        accessor.user = StompPrincipal(userId)
    }

    private fun authorizeSubscribe(accessor: StompHeaderAccessor) {
        if (accessor.user == null) throw MessagingException("unauthorized")
        val destination = accessor.destination ?: throw MessagingException("forbidden-destination")
        if (!isAllowed(destination)) throw MessagingException("forbidden-destination")
    }

    private fun isAllowed(destination: String): Boolean {
        if (destination == Destinations.USER_QUEUE_QUOTE ||
            destination == Destinations.USER_QUEUE_STREAM
        ) {
            return true
        }
        val room = Destinations.parseRoomTopic(destination) ?: return false
        return when (room.kind) {
            ChannelKind.POST -> true
            ChannelKind.TRADE, ChannelKind.DEPTH -> props.features.tradeDepthEnabled
            else -> false
        }
    }

    companion object {
        private const val AUTHORIZATION = "Authorization"
        private const val BEARER_PREFIX = "Bearer "
    }
}
