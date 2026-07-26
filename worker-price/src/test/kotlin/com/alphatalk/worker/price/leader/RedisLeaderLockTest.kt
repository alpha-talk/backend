package com.alphatalk.worker.price.leader

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class RedisLeaderLockTest {
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

    @Test
    fun `리더는 한 인스턴스만 잡고 잡은 쪽은 갱신된다`() {
        val a = RedisLeaderLock(template, "instance-a")
        val b = RedisLeaderLock(template, "instance-b")

        assertTrue(a.tryAcquire())
        assertFalse(b.tryAcquire())
        assertTrue(a.tryAcquire())
    }

    @Test
    fun `해제하면 다른 인스턴스가 인수한다`() {
        val a = RedisLeaderLock(template, "instance-a")
        val b = RedisLeaderLock(template, "instance-b")

        assertTrue(a.tryAcquire())
        a.release()
        assertTrue(b.tryAcquire())
    }

    @Test
    fun `다른 인스턴스의 락은 해제하지 못한다`() {
        val a = RedisLeaderLock(template, "instance-a")
        val b = RedisLeaderLock(template, "instance-b")

        assertTrue(a.tryAcquire())
        b.release()
        assertFalse(b.tryAcquire())
    }

    @Test
    fun `TTL이 만료되면 스탠바이가 인수한다`() {
        val a = RedisLeaderLock(template, "instance-a", ttl = Duration.ofMillis(500))
        val b = RedisLeaderLock(template, "instance-b", ttl = Duration.ofMillis(500))

        assertTrue(a.tryAcquire())
        Thread.sleep(700)
        assertTrue(b.tryAcquire())
    }

    @Test
    fun `해제 후 키가 남지 않는다`() {
        val a = RedisLeaderLock(template, "instance-a")

        assertTrue(a.tryAcquire())
        a.release()

        assertNull(template.opsForValue().get(RedisLeaderLock.LEADER_KEY))
    }
}
