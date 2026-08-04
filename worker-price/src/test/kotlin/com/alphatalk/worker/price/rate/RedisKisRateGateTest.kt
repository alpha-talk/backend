package com.alphatalk.worker.price.rate

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
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
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
        clock: AtomicLong,
    ) = RedisKisRateGate(
        redis = template,
        capacity = capacity,
        refillPerSecond = refill,
        acquireTimeout = Duration.ofMillis(timeoutMillis),
        clock = { clock.get() },
        sleeper = { clock.addAndGet(it) },
    )

    @Test
    fun `용량만큼 즉시 통과하고 초과분은 충전을 기다린다`() {
        val clock = AtomicLong(1_000_000)
        val gate = gate(capacity = 2, refill = 10.0, timeoutMillis = 5_000, clock = clock)

        gate.acquire("key1")
        gate.acquire("key1")
        val before = clock.get()
        gate.acquire("key1")

        assertEquals(true, clock.get() > before)
    }

    @Test
    fun `충전이 타임아웃 안에 오지 않으면 예외를 던진다`() {
        val clock = AtomicLong(1_000_000)
        val gate = gate(capacity = 1, refill = 0.001, timeoutMillis = 300, clock = clock)

        gate.acquire("key1")

        assertFailsWith<KisClientException> { gate.acquire("key1") }
    }

    @Test
    fun `서로 다른 인스턴스가 같은 키의 버킷을 공유한다`() {
        val clock = AtomicLong(1_000_000)
        val first = gate(capacity = 2, refill = 0.001, timeoutMillis = 200, clock = clock)
        val second = gate(capacity = 2, refill = 0.001, timeoutMillis = 200, clock = clock)

        first.acquire("key1")
        second.acquire("key1")

        assertFailsWith<KisClientException> { first.acquire("key1") }
    }

    @Test
    fun `키가 다르면 버킷도 분리된다`() {
        val clock = AtomicLong(1_000_000)
        val gate = gate(capacity = 1, refill = 0.001, timeoutMillis = 200, clock = clock)

        gate.acquire("key1")
        gate.acquire("key2")
    }
}
