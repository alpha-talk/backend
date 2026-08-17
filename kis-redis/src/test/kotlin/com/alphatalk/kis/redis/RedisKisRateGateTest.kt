package com.alphatalk.kis.redis

import com.alphatalk.kis.KisClientException
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import kotlin.test.assertFailsWith

@Testcontainers(disabledWithoutDocker = true)
class RedisKisRateGateTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private val factory by lazy {
            LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }
        }
        private val template by lazy { StringRedisTemplate(factory) }

        private const val HOUR_MILLIS = 3_600_000L
        private const val NEVER_REFILLS = 0.001

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

    private fun gate(
        capacity: Int,
        refill: Double,
        timeoutMillis: Long,
        clockOffsetMillis: Long = 0,
    ) = RedisKisRateGate(
        redis = template,
        capacity = capacity,
        refillPerSecond = refill,
        acquireTimeout = Duration.ofMillis(timeoutMillis),
        clock = { System.currentTimeMillis() + clockOffsetMillis },
    )

    @Test
    fun `용량만큼 즉시 통과하고 초과분은 충전을 기다렸다 통과한다`() {
        val gate = gate(capacity = 2, refill = 100.0, timeoutMillis = 5_000)

        gate.acquire("key1")
        gate.acquire("key1")
        gate.acquire("key1")
    }

    @Test
    fun `충전이 타임아웃 안에 오지 않으면 예외를 던진다`() {
        val gate = gate(capacity = 1, refill = NEVER_REFILLS, timeoutMillis = 200)

        gate.acquire("key1")

        assertFailsWith<KisClientException> { gate.acquire("key1") }
    }

    @Test
    fun `서로 다른 인스턴스가 같은 키의 버킷을 공유한다`() {
        val first = gate(capacity = 2, refill = NEVER_REFILLS, timeoutMillis = 200)
        val second = gate(capacity = 2, refill = NEVER_REFILLS, timeoutMillis = 200)

        first.acquire("key1")
        second.acquire("key1")

        assertFailsWith<KisClientException> { first.acquire("key1") }
    }

    @Test
    fun `인스턴스 시계가 어긋나도 Redis 시계 하나로 계산해 한도를 넘지 않는다`() {
        val behind = gate(
            capacity = 2,
            refill = NEVER_REFILLS,
            timeoutMillis = 200,
            clockOffsetMillis = -HOUR_MILLIS,
        )
        val ahead = gate(
            capacity = 2,
            refill = NEVER_REFILLS,
            timeoutMillis = 200,
            clockOffsetMillis = HOUR_MILLIS,
        )

        behind.acquire("key1")
        behind.acquire("key1")

        assertFailsWith<KisClientException> { ahead.acquire("key1") }
        assertFailsWith<KisClientException> { behind.acquire("key1") }
    }

    @Test
    fun `키가 다르면 버킷도 분리된다`() {
        val gate = gate(capacity = 1, refill = NEVER_REFILLS, timeoutMillis = 200)

        gate.acquire("key1")
        gate.acquire("key2")
    }
}
