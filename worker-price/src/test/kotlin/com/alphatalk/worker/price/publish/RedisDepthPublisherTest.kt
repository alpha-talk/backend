package com.alphatalk.worker.price.publish

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.envelope.DepthData
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
class RedisDepthPublisherTest {
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
    private val depthData = DepthData(
        bids = listOf(listOf(71200, 340), listOf(71100, 280)),
        asks = listOf(listOf(71300, 120), listOf(71400, 250)),
    )

    @BeforeEach
    fun flush() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
    }

    @Test
    fun `depth 채널로 eventId 없는 봉투를 발행하고 캐시 키는 만들지 않는다`() {
        val received = CopyOnWriteArrayList<String>()
        listenerContainer.addMessageListener(
            { message, _ -> received += String(message.body) },
            ChannelTopic(Channels.depth("005930")),
        )
        val publisher = RedisDepthPublisher(template, mapper)

        await().atMost(Duration.ofSeconds(5)).until {
            publisher.publish("005930", depthData, ts = 1719500000000)
            received.isNotEmpty()
        }

        val envelope = mapper.readTree(received[0])
        assertEquals("depth", envelope.path("type").asText())
        assertEquals("005930", envelope.path("code").asText())
        assertEquals(1719500000000, envelope.path("ts").asLong())
        assertFalse(envelope.has("eventId"))
        assertEquals(71200, envelope.path("data").path("bids")[0][0].asLong())
        assertEquals(340, envelope.path("data").path("bids")[0][1].asLong())
        assertEquals(71300, envelope.path("data").path("asks")[0][0].asLong())
        assertTrue(template.keys("*").isNullOrEmpty())
    }

    @Test
    fun `Redis 순단 시 예외를 삼키고 다음 호가를 기다린다`() {
        val brokenFactory = LettuceConnectionFactory("localhost", 1).apply {
            afterPropertiesSet()
        }
        val publisher = RedisDepthPublisher(StringRedisTemplate(brokenFactory), mapper)

        publisher.publish("005930", depthData, ts = 1719500000000)

        assertTrue(true)
        brokenFactory.destroy()
    }
}
