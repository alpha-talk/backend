package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class RedisWatchlistMirrorSyncTest {
    companion object {
        private const val PROBE = "probe"
        private val NOW: Instant = Instant.parse("2026-07-31T00:00:00Z")
        private val USER_SEQ = AtomicLong()

        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private lateinit var factory: LettuceConnectionFactory
        private lateinit var template: StringRedisTemplate
        private lateinit var listener: RedisMessageListenerContainer
        private val published = LinkedBlockingQueue<String>()

        @JvmStatic
        @BeforeAll
        fun connect() {
            factory = LettuceConnectionFactory(redis.host, redis.getMappedPort(6379))
            factory.afterPropertiesSet()
            template = StringRedisTemplate(factory)
            listener = RedisMessageListenerContainer()
            listener.setConnectionFactory(factory)
            listener.afterPropertiesSet()
            listener.start()
            listener.addMessageListener(
                { message, _ -> published += String(message.body) },
                ChannelTopic(Channels.WATCHLIST_UPDATED),
            )
            awaitSubscription()
        }

        @JvmStatic
        @AfterAll
        fun disconnect() {
            listener.stop()
            listener.destroy()
            factory.destroy()
        }

        private fun awaitSubscription() {
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
            while (System.nanoTime() < deadline) {
                template.convertAndSend(Channels.WATCHLIST_UPDATED, PROBE)
                if (published.poll(200, TimeUnit.MILLISECONDS) != null) {
                    published.clear()
                    return
                }
            }
            throw IllegalStateException("watchlist:updated 구독이 시작되지 않았다")
        }
    }

    private val mapper = ObjectMapper()
    private val sync = RedisWatchlistMirrorSync(template, Clock.fixed(NOW, ZoneOffset.UTC))
    private var userId = 0L

    @BeforeEach
    fun reset() {
        userId = USER_SEQ.incrementAndGet()
        published.clear()
    }

    private fun mirrored(): Set<String> = template.opsForSet().members(Keys.watchlist(userId)).orEmpty()

    private fun nextEvent(): JsonNode? = published.poll(3, TimeUnit.SECONDS)?.let(mapper::readTree)

    @Test
    fun `첫 동기화는 스냅샷 전체를 미러에 반영하고 diff를 발행한다`() {
        val applied = sync.sync(userId, MirrorChange(WatchlistState(1, listOf("005930", "000660")), addedCode = "005930"))

        assertTrue(applied)
        assertEquals(setOf("005930", "000660"), mirrored())
        assertEquals("1", template.opsForValue().get(Keys.watchlistRev(userId)))
        val event = nextEvent()!!
        assertEquals(userId, event.path("userId").asLong())
        assertEquals(setOf("005930", "000660"), event.path("added").map { it.asText() }.toSet())
        assertTrue(event.path("removed").isEmpty)
        assertEquals(NOW.toEpochMilli(), event.path("ts").asLong())
    }

    @Test
    fun `오래된 rev는 미러 교체도 발행도 하지 않는다`() {
        sync.sync(userId, MirrorChange(WatchlistState(5, listOf("005930")), addedCode = "005930"))
        nextEvent()

        val applied = sync.sync(userId, MirrorChange(WatchlistState(4, emptyList()), removedCode = "005930"))

        assertFalse(applied)
        assertEquals(setOf("005930"), mirrored())
        assertEquals("5", template.opsForValue().get(Keys.watchlistRev(userId)))
        assertNull(nextEvent())
    }

    @Test
    fun `새 rev는 미러를 통째로 교체하고 전이 diff를 발행한다`() {
        sync.sync(userId, MirrorChange(WatchlistState(5, listOf("005930", "000660")), addedCode = "000660"))
        nextEvent()

        val applied = sync.sync(userId, MirrorChange(WatchlistState(6, listOf("000660", "000440")), addedCode = "000440"))

        assertTrue(applied)
        assertEquals(setOf("000660", "000440"), mirrored())
        val event = nextEvent()!!
        assertEquals(setOf("000440"), event.path("added").map { it.asText() }.toSet())
        assertEquals(setOf("005930"), event.path("removed").map { it.asText() }.toSet())
    }

    @Test
    fun `미러가 이미 맞아도 재요청 diff는 발행한다`() {
        sync.sync(userId, MirrorChange(WatchlistState(5, listOf("005930")), addedCode = "005930"))
        nextEvent()

        val applied = sync.sync(userId, MirrorChange(WatchlistState(6, listOf("005930")), addedCode = "005930"))

        assertTrue(applied)
        assertEquals(setOf("005930"), mirrored())
        val event = nextEvent()!!
        assertEquals(listOf("005930"), event.path("added").map { it.asText() })
        assertTrue(event.path("removed").isEmpty)
    }

    @Test
    fun `빈 스냅샷은 미러를 비우고 해지 diff를 발행한다`() {
        sync.sync(userId, MirrorChange(WatchlistState(5, listOf("005930")), addedCode = "005930"))
        nextEvent()

        val applied = sync.sync(userId, MirrorChange(WatchlistState(6, emptyList()), removedCode = "005930"))

        assertTrue(applied)
        assertTrue(mirrored().isEmpty())
        val event = nextEvent()!!
        assertTrue(event.path("added").isEmpty)
        assertEquals(listOf("005930"), event.path("removed").map { it.asText() })
    }
}
