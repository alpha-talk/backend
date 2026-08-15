package com.alphatalk.ws.auth

import com.alphatalk.auth.InvalidTokenException
import com.alphatalk.auth.TokenVerifier
import com.alphatalk.ws.config.WsProperties
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.springframework.messaging.Message
import org.springframework.messaging.MessagingException
import org.springframework.messaging.simp.stomp.StompCommand
import org.springframework.messaging.simp.stomp.StompHeaderAccessor
import org.springframework.messaging.support.MessageBuilder

class StompAuthChannelInterceptorTest {
    private class FakeVerifier : TokenVerifier {
        override fun verify(token: String): Long =
            if (token == "valid-token") 42L else throw InvalidTokenException("nope")
    }

    private fun props(tradeDepth: Boolean = false) = WsProperties(
        features = WsProperties.Features(tradeDepthEnabled = tradeDepth),
    )

    private val channel = org.springframework.messaging.support.ExecutorSubscribableChannel()

    private fun interceptor(tradeDepth: Boolean = false) =
        StompAuthChannelInterceptor(
            FakeVerifier(),
            props(tradeDepth),
            io.micrometer.core.instrument.simple.SimpleMeterRegistry(),
        )

    private fun connectMessage(authHeader: String?): Message<ByteArray> {
        val accessor = StompHeaderAccessor.create(StompCommand.CONNECT)
        authHeader?.let { accessor.setNativeHeader("Authorization", it) }
        accessor.setLeaveMutable(true)
        return MessageBuilder.createMessage(ByteArray(0), accessor.messageHeaders)
    }

    private fun subscribeMessage(destination: String, authenticated: Boolean): Message<ByteArray> {
        val accessor = StompHeaderAccessor.create(StompCommand.SUBSCRIBE)
        accessor.destination = destination
        accessor.subscriptionId = "sub-1"
        if (authenticated) accessor.user = StompPrincipal(42L)
        accessor.setLeaveMutable(true)
        return MessageBuilder.createMessage(ByteArray(0), accessor.messageHeaders)
    }

    @Test
    fun `유효 토큰 CONNECT - Principal 부여`() {
        val message = interceptor().preSend(connectMessage("Bearer valid-token"), channel)

        val user = StompHeaderAccessor.wrap(message!!).user
        assertThat(user).isEqualTo(StompPrincipal(42L))
        assertThat(user!!.name).isEqualTo("42")
    }

    @Test
    fun `무효 토큰 - unauthorized`() {
        assertThatThrownBy { interceptor().preSend(connectMessage("Bearer bad-token"), channel) }
            .isInstanceOf(MessagingException::class.java)
            .hasMessageContaining("unauthorized")
    }

    @Test
    fun `Authorization 헤더 없음 - unauthorized`() {
        assertThatThrownBy { interceptor().preSend(connectMessage(null), channel) }
            .isInstanceOf(MessagingException::class.java)
    }

    @Test
    fun `Bearer 형식 아님 - unauthorized`() {
        assertThatThrownBy { interceptor().preSend(connectMessage("Basic abc"), channel) }
            .isInstanceOf(MessagingException::class.java)
    }

    @Test
    fun `허용 목적지 - 통과`() {
        val allowed = listOf(
            "/user/queue/quote",
            "/user/queue/stream",
            "/topic/rooms/005930/quote",
            "/topic/rooms/005930/posts",
        )
        for (dest in allowed) {
            interceptor().preSend(subscribeMessage(dest, authenticated = true), channel)
        }
    }

    @Test
    fun `미인증 SUBSCRIBE - 거부 (CONNECTED 이전 구독 차단)`() {
        assertThatThrownBy {
            interceptor().preSend(subscribeMessage("/user/queue/quote", authenticated = false), channel)
        }.isInstanceOf(MessagingException::class.java).hasMessageContaining("unauthorized")
    }

    @Test
    fun `화이트리스트 밖 목적지 - 거부`() {
        val forbidden = listOf("/topic/other", "/queue/quote", "/topic/rooms/12345/posts", "/user/queue/etc")
        for (dest in forbidden) {
            assertThatThrownBy { interceptor().preSend(subscribeMessage(dest, authenticated = true), channel) }
                .`as`("destination: %s", dest)
                .isInstanceOf(MessagingException::class.java)
        }
    }

    @Test
    fun `trade-depth 플래그 - off면 거부, on이면 허용`() {
        assertThatThrownBy {
            interceptor(tradeDepth = false)
                .preSend(subscribeMessage("/topic/rooms/005930/trade", authenticated = true), channel)
        }.isInstanceOf(MessagingException::class.java)

        interceptor(tradeDepth = true)
            .preSend(subscribeMessage("/topic/rooms/005930/trade", authenticated = true), channel)
        interceptor(tradeDepth = true)
            .preSend(subscribeMessage("/topic/rooms/005930/depth", authenticated = true), channel)
    }

    @Test
    fun `SEND는 무조건 거부 - 푸시 전용 엣지`() {
        val accessor = StompHeaderAccessor.create(StompCommand.SEND)
        accessor.destination = "/topic/rooms/005930/posts"
        accessor.user = StompPrincipal(42L)
        val message = MessageBuilder.createMessage("payload".toByteArray(), accessor.messageHeaders)

        assertThatThrownBy { interceptor().preSend(message, channel) }
            .isInstanceOf(MessagingException::class.java)
            .hasMessageContaining("send-not-allowed")
    }
}
