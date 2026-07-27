package com.alphatalk.worker.price.redis

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@Testcontainers(disabledWithoutDocker = true)
class RedisKisTokenStoreTest {
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

    private val store by lazy { RedisKisTokenStore(template) }

    @BeforeEach
    fun flush() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
    }

    @Test
    fun `락은 소유 토큰으로만 해제된다`() {
        val lockToken = store.tryLock("key1", Duration.ofSeconds(10))
        assertNotNull(lockToken)

        store.unlock("key1", "wrong-token")
        assertNull(store.tryLock("key1", Duration.ofSeconds(10)))

        store.unlock("key1", lockToken)
        assertNotNull(store.tryLock("key1", Duration.ofSeconds(10)))
    }

    @Test
    fun `만료 후 넘어간 락은 이전 소유자의 해제로 지워지지 않는다`() {
        val first = store.tryLock("key1", Duration.ofMillis(300))
        assertNotNull(first)
        Thread.sleep(400)

        val second = store.tryLock("key1", Duration.ofSeconds(10))
        assertNotNull(second)

        store.unlock("key1", first)

        assertNull(store.tryLock("key1", Duration.ofSeconds(10)))
        store.unlock("key1", second)
        assertNotNull(store.tryLock("key1", Duration.ofSeconds(10)))
    }

    @Test
    fun `토큰과 발급 시각을 왕복 저장한다`() {
        store.put("key1", "T1", Duration.ofMinutes(5))
        assertEquals("T1", store.get("key1"))
        store.evict("key1")
        assertNull(store.get("key1"))

        val issuedAt = Instant.parse("2026-07-27T00:00:00Z")
        store.markIssued("key1", issuedAt)
        assertEquals(issuedAt, store.lastIssuedAt("key1"))
    }
}
