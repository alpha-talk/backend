package com.alphatalk.ws

import com.alphatalk.auth.JwtTokenProvider
import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Destinations
import com.alphatalk.contracts.Keys
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.messaging.simp.broker.OrderedMessageChannelDecorator
import org.springframework.messaging.simp.stomp.StompHeaders
import org.springframework.messaging.simp.stomp.StompSession
import org.springframework.messaging.simp.stomp.StompSessionHandlerAdapter
import org.springframework.messaging.support.AbstractSubscribableChannel
import org.springframework.web.socket.WebSocketHttpHeaders
import org.springframework.web.socket.client.standard.StandardWebSocketClient
import org.springframework.web.socket.messaging.StompSubProtocolHandler
import org.springframework.web.socket.messaging.SubProtocolWebSocketHandler
import org.springframework.web.socket.messaging.WebSocketStompClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.lang.reflect.Type
import java.time.Duration
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["alphatalk.auth.jwt.secret=$TEST_SECRET"],
)
@Testcontainers(disabledWithoutDocker = true)
class WsIntegrationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)
    }

    @LocalServerPort
    private var port = 0

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var subProtocolWebSocketHandler: SubProtocolWebSocketHandler

    @Autowired
    @Qualifier("clientInboundChannel")
    private lateinit var clientInboundChannel: AbstractSubscribableChannel

    private val sessions = mutableListOf<StompSession>()

    @AfterEach
    fun cleanup() {
        sessions.forEach { runCatching { it.disconnect() } }
        sessions.clear()
        redisTemplate.execute { connection -> connection.serverCommands().flushAll() }
    }

    private val tokenProvider = JwtTokenProvider(TEST_SECRET)

    private fun token(userId: Long): String = tokenProvider.issue(userId, Duration.ofMinutes(5))

    private fun connect(userId: Long): StompSession {
        val client = WebSocketStompClient(StandardWebSocketClient())
        val connectHeaders = StompHeaders().apply { add("Authorization", "Bearer ${token(userId)}") }
        val session = client.connectAsync(
            "ws://localhost:$port/ws",
            null as WebSocketHttpHeaders?,
            connectHeaders,
            object : StompSessionHandlerAdapter() {},
        ).get(5, TimeUnit.SECONDS)
        sessions += session
        return session
    }

    private fun subscribeQueue(session: StompSession, destination: String): BlockingQueue<String> {
        val received = LinkedBlockingQueue<String>()
        session.subscribe(
            destination,
            object : StompSessionHandlerAdapter() {
                override fun getPayloadType(headers: StompHeaders): Type = ByteArray::class.java
                override fun handleFrame(headers: StompHeaders, payload: Any?) {
                    received.offer(String(payload as ByteArray, Charsets.UTF_8))
                }
            },
        )
        return received
    }

    private fun publishUntilReceived(channel: String, body: String, queue: BlockingQueue<String>): String {
        var message: String? = null
        await().atMost(Duration.ofSeconds(10)).until {
            redisTemplate.convertAndSend(channel, body)
            message = queue.poll(300, TimeUnit.MILLISECONDS)
            message != null
        }
        return message!!
    }

    @Test
    fun `관심목록 quote relay - 접속하면 관심 종목 틱이 user queue로 온다`() {
        redisTemplate.opsForSet().add(Keys.watchlist(1L), "005930")

        val session = connect(1L)
        val received = subscribeQueue(session, Destinations.USER_QUEUE_QUOTE)

        val body = """{"type":"quote","code":"005930","ts":1719600000000,"data":{"price":71200}}"""
        val message = publishUntilReceived(Channels.quote("005930"), body, received)

        assertThat(message).contains(""""code":"005930"""").contains(""""price":71200""")
    }

    @Test
    fun `방 post relay - 방 토픽 구독 세션에 글이 도착한다`() {
        val session = connect(2L)
        val received = subscribeQueue(session, Destinations.roomPosts("005930"))

        val body =
            """{"type":"post","code":"005930","eventId":"01J","ts":1,"data":{"kind":"post","content":"hi"}}"""
        val message = publishUntilReceived(Channels.post("005930"), body, received)

        assertThat(message).contains(""""eventId":"01J"""")
    }

    @Test
    fun `watchlist updated - 재접속 없이 새 종목 틱이 흐른다 (FR-03)`() {
        redisTemplate.opsForSet().add(Keys.watchlist(3L), "005930")
        val session = connect(3L)
        val received = subscribeQueue(session, Destinations.USER_QUEUE_QUOTE)

        publishUntilReceived(
            Channels.quote("005930"),
            """{"type":"quote","code":"005930","ts":1,"data":{}}""",
            received,
        )

        redisTemplate.convertAndSend(
            Channels.WATCHLIST_UPDATED,
            """{"userId":3,"added":["000660"],"removed":[],"ts":1}""",
        )

        val message = publishUntilReceived(
            Channels.quote("000660"),
            """{"type":"quote","code":"000660","ts":2,"data":{"price":180000}}""",
            received,
        )
        assertThat(message).contains(""""code":"000660"""")
    }

    @Test
    fun `수신 순서 보장 결선 - 핸들러 플래그와 채널 인터셉터가 함께 걸려 있다`() {
        val stompHandler = subProtocolWebSocketHandler.protocolHandlers
            .filterIsInstance<StompSubProtocolHandler>()
            .single()

        assertThat(stompHandler.isPreserveReceiveOrder).isTrue()
        assertThat(OrderedMessageChannelDecorator.supportsOrderedMessages(clientInboundChannel)).isTrue()
    }

    @Test
    fun `무효 토큰 - 연결 실패`() {
        val client = WebSocketStompClient(StandardWebSocketClient())
        val connectHeaders = StompHeaders().apply { add("Authorization", "Bearer garbage") }
        val future = client.connectAsync(
            "ws://localhost:$port/ws",
            null as WebSocketHttpHeaders?,
            connectHeaders,
            object : StompSessionHandlerAdapter() {},
        )
        assertThrows<Exception> { future.get(5, TimeUnit.SECONDS) }
    }
}

private const val TEST_SECRET = "integration-test-secret-0123456789abcdef"
