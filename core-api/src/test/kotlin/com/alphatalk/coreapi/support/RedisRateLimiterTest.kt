package com.alphatalk.coreapi.support

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
@ActiveProfiles("test")
class RedisRateLimiterTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    private val now = Instant.parse("2026-07-30T09:00:10Z")

    @BeforeEach
    fun clear() {
        redisTemplate.connectionFactory!!.connection.use { it.serverCommands().flushDb() }
    }

    private fun limiter(at: Instant = now) = RedisRateLimiter(redisTemplate, Clock.fixed(at, ZoneOffset.UTC))

    @Test
    fun `한도까지 허용하고 그다음부터 거절한다`() {
        val limiter = limiter()

        val decisions = (1..4).map { limiter.tryAcquire("post", "1", 3, Duration.ofMinutes(1)) }

        assertEquals(listOf(true, true, true, false), decisions.map(RateLimitDecision::allowed))
        assertTrue(decisions.last().retryAfterSeconds in 1..60)
    }

    @Test
    fun `다음 고정 윈도에서는 다시 허용한다`() {
        limiter().let { first ->
            repeat(3) { first.tryAcquire("post", "1", 3, Duration.ofMinutes(1)) }
            assertFalse(first.tryAcquire("post", "1", 3, Duration.ofMinutes(1)).allowed)
        }

        val nextWindow = limiter(now.plusSeconds(60))

        assertTrue(nextWindow.tryAcquire("post", "1", 3, Duration.ofMinutes(1)).allowed)
    }

    @Test
    fun `행동과 키가 다르면 카운터가 분리된다`() {
        val limiter = limiter()
        repeat(3) { limiter.tryAcquire("post", "1", 3, Duration.ofMinutes(1)) }

        assertTrue(limiter.tryAcquire("comment", "1", 3, Duration.ofMinutes(1)).allowed)
        assertTrue(limiter.tryAcquire("post", "2", 3, Duration.ofMinutes(1)).allowed)
    }

    @Test
    fun `윈도 카운터 키에는 TTL이 걸린다`() {
        limiter().tryAcquire("post", "1", 3, Duration.ofMinutes(1))

        val key = redisTemplate.keys("rl:post:1:*").single()
        val ttl = redisTemplate.getExpire(key)

        assertTrue(ttl in 1..60, "TTL이 비정상이다: $ttl")
    }
}
