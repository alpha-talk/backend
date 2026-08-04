package com.alphatalk.ws.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.DemandUpdated
import com.alphatalk.ws.config.WsProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

@Testcontainers(disabledWithoutDocker = true)
class RedisDemandSignalPublisherTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private val factory by lazy {
            LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }
        }
        private val template by lazy { StringRedisTemplate(factory) }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            factory.destroy()
        }
    }

    private val objectMapper = jacksonObjectMapper()
    private val publisher by lazy { RedisDemandSignalPublisher(template, objectMapper, WsProperties()) }
    private val published = LinkedBlockingQueue<DemandUpdated>()
    private lateinit var listenerContainer: RedisMessageListenerContainer

    @BeforeEach
    fun setUp() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
        listenerContainer = RedisMessageListenerContainer().apply {
            setConnectionFactory(factory)
            addMessageListener(
                { message, _ -> published.add(objectMapper.readValue(message.body, DemandUpdated::class.java)) },
                ChannelTopic(Channels.DEMAND_UPDATED),
            )
            afterPropertiesSet()
            start()
        }
    }

    @AfterEach
    fun stopListener() {
        listenerContainer.destroy()
    }

    private fun quoteCount(code: String): String? =
        template.opsForHash<String, String>().get(Keys.demandQuote(publisher.gwId), code)

    @Test
    fun `첫 증가 - refcount 1이 되고 active true를 발행한다`() {
        publisher.increment(DemandSignalKind.QUOTE, "005930")

        assertThat(quoteCount("005930")).isEqualTo("1")
        val message = published.poll(5, TimeUnit.SECONDS)
        assertThat(message).isEqualTo(
            DemandUpdated(kind = DemandUpdated.KIND_QUOTE, code = "005930", active = true, ts = message!!.ts),
        )
    }

    @Test
    fun `0이 아닌 전이 - 발행하지 않는다`() {
        publisher.increment(DemandSignalKind.QUOTE, "005930")
        published.poll(5, TimeUnit.SECONDS)

        publisher.increment(DemandSignalKind.QUOTE, "005930")
        publisher.decrement(DemandSignalKind.QUOTE, "005930")

        assertThat(quoteCount("005930")).isEqualTo("1")
        assertThat(published.poll(500, TimeUnit.MILLISECONDS)).isNull()
    }

    @Test
    fun `마지막 감소 - 필드가 제거되고 active false를 발행한다`() {
        publisher.increment(DemandSignalKind.ROOM, "005930")
        published.poll(5, TimeUnit.SECONDS)

        publisher.decrement(DemandSignalKind.ROOM, "005930")

        assertThat(template.opsForHash<String, String>().get(Keys.demandRoom(publisher.gwId), "005930")).isNull()
        val message = published.poll(5, TimeUnit.SECONDS)
        assertThat(message?.kind).isEqualTo(DemandUpdated.KIND_ROOM)
        assertThat(message?.active).isFalse()
    }

    @Test
    fun `수요 없는 감소(underflow) - 발행 없이 필드만 정리한다`() {
        publisher.decrement(DemandSignalKind.QUOTE, "005930")

        assertThat(quoteCount("005930")).isNull()
        assertThat(published.poll(500, TimeUnit.MILLISECONDS)).isNull()
    }

    @Test
    fun `하트비트 - gw alive를 갱신하고 수요 해시 TTL을 연장한다`() {
        publisher.increment(DemandSignalKind.QUOTE, "005930")
        publisher.heartbeat()

        assertThat(template.opsForValue().get(Keys.gwAlive(publisher.gwId))).isNotNull()
        assertThat(template.getExpire(Keys.gwAlive(publisher.gwId))).isBetween(1L, 15L)
        assertThat(template.getExpire(Keys.demandQuote(publisher.gwId))).isBetween(1L, 60L)
    }

    @Test
    fun `withdraw - 자기 키를 모두 삭제한다`() {
        publisher.increment(DemandSignalKind.QUOTE, "005930")
        publisher.increment(DemandSignalKind.ROOM, "005930")
        publisher.heartbeat()

        publisher.withdraw()

        assertThat(template.hasKey(Keys.gwAlive(publisher.gwId))).isFalse()
        assertThat(template.hasKey(Keys.demandQuote(publisher.gwId))).isFalse()
        assertThat(template.hasKey(Keys.demandRoom(publisher.gwId))).isFalse()
    }
}
