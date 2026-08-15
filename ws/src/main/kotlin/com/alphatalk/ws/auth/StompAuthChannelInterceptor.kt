package com.alphatalk.ws.auth

import com.alphatalk.auth.InvalidTokenException
import com.alphatalk.auth.TokenVerifier
import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Destinations
import com.alphatalk.ws.config.WsProperties
import io.micrometer.core.instrument.MeterRegistry
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
    meterRegistry: MeterRegistry,
) : ChannelInterceptor {
    private val log = LoggerFactory.getLogger(javaClass)
    private val stompErrors = meterRegistry.counter("ws.stomp.errors")

    override fun preSend(message: Message<*>, channel: MessageChannel): Message<*> {
        val accessor = MessageHeaderAccessor.getAccessor(message, StompHeaderAccessor::class.java)
            ?: return message
        when (accessor.command) {
            StompCommand.CONNECT -> authenticate(accessor)
            StompCommand.SUBSCRIBE -> authorizeSubscribe(accessor)
            StompCommand.SEND -> reject("send-not-allowed")
            else -> Unit
        }
        return message
    }

    private fun authenticate(accessor: StompHeaderAccessor) {
        val header = accessor.getFirstNativeHeader(AUTHORIZATION) ?: reject("unauthorized")
        if (!header.startsWith(BEARER_PREFIX)) reject("unauthorized")
        val userId = try {
            tokenVerifier.verify(header.removePrefix(BEARER_PREFIX).trim())
        } catch (e: InvalidTokenException) {
            log.info("CONNECT rejected: invalid token ({})", e.message)
            reject("unauthorized")
        }
        accessor.user = StompPrincipal(userId)
    }

    private fun authorizeSubscribe(accessor: StompHeaderAccessor) {
        if (accessor.user == null) reject("unauthorized")
        val destination = accessor.destination ?: reject("forbidden-destination")
        if (!isAllowed(destination)) reject("forbidden-destination")
    }

    private fun reject(reason: String): Nothing {
        stompErrors.increment()
        throw MessagingException(reason)
    }

    private fun isAllowed(destination: String): Boolean {
        if (destination == Destinations.USER_QUEUE_QUOTE ||
            destination == Destinations.USER_QUEUE_STREAM
        ) {
            return true
        }
        val room = Destinations.parseRoomTopic(destination) ?: return false
        return when (room.kind) {
            ChannelKind.QUOTE, ChannelKind.POST -> true
            ChannelKind.TRADE, ChannelKind.DEPTH -> props.features.tradeDepthEnabled
            else -> false
        }
    }

    companion object {
        private const val AUTHORIZATION = "Authorization"
        private const val BEARER_PREFIX = "Bearer "
    }
}
