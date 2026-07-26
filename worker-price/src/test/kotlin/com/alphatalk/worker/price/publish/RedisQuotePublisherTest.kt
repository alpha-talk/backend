package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.envelope.QuoteData
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class RedisQuotePublisherTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private val factory by lazy {
            LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }
        }
        private val template by lazy { StringRedisTemplate(factory) }
        private val listenerContainer by lazy {
            RedisMessageListenerContainer().apply {
                setConnectionFactory(factory)
                afterPropertiesSet()
                start()
            }
        }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            listenerContainer.stop()
            factory.destroy()
        }
    }

    private val mapper = jacksonObjectMapper()
    private val quoteData = QuoteData(
        price = 71200,
        prevClose = 70500,
        change = 700,
        changeRate = 0.99,
        volume = 1234567,
        open = 70600,
        high = 71500,
        low = 70400,
    )

    @BeforeEach
    fun flush() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
    }

    @Test
    fun `price 해시에 9필드를 쓴다`() {
        val publisher = RedisQuotePublisher(template, mapper)

        publisher.publish("005930", quoteData, ts = 1719500000000)

        val hash = template.opsForHash<String, String>().entries(Keys.price("005930"))
        assertEquals(
            mapOf(
                "price" to "71200",
                "prevClose" to "70500",
                "change" to "700",
                "changeRate" to "0.99",
                "open" to "70600",
                "high" to "71500",
                "low" to "70400",
                "volume" to "1234567",
                "ts" to "1719500000000",
            ),
            hash,
        )
        assertEquals(-1, template.getExpire(Keys.price("005930")))
    }

    @Test
    fun `quote 채널로 eventId 없는 봉투를 발행한다`() {
        val received = CopyOnWriteArrayList<String>()
        listenerContainer.addMessageListener(
            { message, _ -> received += String(message.body) },
            ChannelTopic(Channels.quote("005930")),
        )
        val publisher = RedisQuotePublisher(template, mapper)

        await().atMost(Duration.ofSeconds(5)).until {
            publisher.publish("005930", quoteData, ts = 1719500000000)
            received.isNotEmpty()
        }

        val envelope = mapper.readTree(received[0])
        assertEquals("quote", envelope.path("type").asText())
        assertEquals("005930", envelope.path("code").asText())
        assertEquals(1719500000000, envelope.path("ts").asLong())
        assertFalse(envelope.has("eventId"))
        assertEquals(71200, envelope.path("data").path("price").asLong())
        assertEquals(70500, envelope.path("data").path("prevClose").asLong())
        assertEquals(0.99, envelope.path("data").path("changeRate").asDouble())
    }

    @Test
    fun `Redis 순단 시 예외를 삼키고 다음 틱을 기다린다`() {
        val brokenFactory = LettuceConnectionFactory("localhost", 1).apply {
            afterPropertiesSet()
        }
        val publisher = RedisQuotePublisher(StringRedisTemplate(brokenFactory), mapper)

        publisher.publish("005930", quoteData, ts = 1719500000000)

        assertTrue(true)
        brokenFactory.destroy()
    }
}
