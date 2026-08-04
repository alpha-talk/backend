package com.alphatalk.worker.price.demand

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import kotlin.test.assertEquals

@Testcontainers(disabledWithoutDocker = true)
class RedisDemandSourceTest {
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

    @BeforeEach
    fun flush() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
    }

    private fun markAlive(gwId: String) {
        template.opsForValue().set(Keys.gwAlive(gwId), "1", Duration.ofSeconds(15))
    }

    @Test
    fun `alive 게이트웨이의 quote·room 수요를 합산하고 base와 합집합한다`() {
        markAlive("gw1")
        template.opsForHash<String, String>().put(Keys.demandQuote("gw1"), "000660", "2")
        template.opsForHash<String, String>().put(Keys.demandQuote("gw1"), "999999", "0")
        template.opsForHash<String, String>().put(Keys.demandRoom("gw1"), "035420", "1")

        val source = RedisDemandSource(template, factory, baseSymbols = setOf("005930"))
        source.refresh()

        assertEquals(setOf("005930", "000660", "035420"), source.targetSymbols())
    }

    @Test
    fun `gw alive가 없는 게이트웨이의 수요는 스테일로 보고 제외한다`() {
        template.opsForHash<String, String>().put(Keys.demandQuote("gw-dead"), "000660", "3")

        val source = RedisDemandSource(template, factory, baseSymbols = setOf("005930"))
        source.refresh()

        assertEquals(setOf("005930"), source.targetSymbols())
    }

    @Test
    fun `여러 게이트웨이 수요를 합산한다`() {
        markAlive("gw1")
        markAlive("gw2")
        template.opsForHash<String, String>().put(Keys.demandQuote("gw1"), "000660", "1")
        template.opsForHash<String, String>().put(Keys.demandQuote("gw2"), "035420", "1")

        val source = RedisDemandSource(template, factory, baseSymbols = emptySet())
        source.refresh()

        assertEquals(setOf("000660", "035420"), source.targetSymbols())
    }

    @Test
    fun `demand updated 수신 - 즉시 리컨실해 수요 등장과 소멸을 반영한다`() {
        val source = RedisDemandSource(template, factory, baseSymbols = emptySet(), reconcileIntervalMs = 600_000)
        source.start()
        try {
            markAlive("gw1")
            template.opsForHash<String, String>().put(Keys.demandQuote("gw1"), "000660", "1")
            template.convertAndSend(Channels.DEMAND_UPDATED, """{"kind":"quote","code":"000660","active":true,"ts":1}""")
            await().atMost(Duration.ofSeconds(5)).until { source.targetSymbols() == setOf("000660") }

            template.opsForHash<String, String>().delete(Keys.demandQuote("gw1"), "000660")
            template.convertAndSend(Channels.DEMAND_UPDATED, """{"kind":"quote","code":"000660","active":false,"ts":2}""")
            await().atMost(Duration.ofSeconds(5)).until { source.targetSymbols().isEmpty() }
        } finally {
            source.stop()
        }
    }
}
