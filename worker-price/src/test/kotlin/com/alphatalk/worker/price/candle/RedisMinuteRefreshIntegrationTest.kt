package com.alphatalk.worker.price.candle

import com.alphatalk.contracts.Keys
import com.alphatalk.kis.rest.KisMinuteCandle
import com.alphatalk.kis.rest.KisMinuteChart
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.candle.MinuteRefreshLock.Result
import com.alphatalk.worker.price.config.PriceRedisListenerConfig
import com.alphatalk.worker.price.demand.RedisDemandSource
import com.alphatalk.worker.price.market.InMemoryMarketDivStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.mockito.Mockito
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveStringRedisTemplate
import org.springframework.data.redis.core.ReactiveValueOperations
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.time.ZonedDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class RedisMinuteRefreshIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)
    }

    private val code = "005930"
    private val now = ZonedDateTime.parse("2026-08-04T08:02:00+09:00[Asia/Seoul]")
    private lateinit var ownerConnection: LettuceConnectionFactory
    private lateinit var followerConnection: LettuceConnectionFactory
    private lateinit var commands: StringRedisTemplate
    private lateinit var owner: RedisMinuteRefreshLock

    @BeforeTest
    fun connect() {
        ownerConnection = connectionFactory()
        followerConnection = connectionFactory()
        commands = StringRedisTemplate(ownerConnection)
        commands.delete(Keys.minuteRefreshLock(code))
        owner = RedisMinuteRefreshLock(commands, ReactiveStringRedisTemplate(ownerConnection))
    }

    @AfterTest
    fun disconnect() {
        followerConnection.destroy()
        ownerConnection.destroy()
    }

    private fun connectionFactory() = LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply {
        afterPropertiesSet()
    }

    @Test
    fun `다른 워커는 저장 반환 후의 완료 알림으로 깨어나고 같은 적재 건수를 받는다`() {
        val upsertStarted = CountDownLatch(1)
        val allowCommit = CountDownLatch(1)
        val firstRead = CountDownLatch(1)
        val calls = AtomicInteger()
        val store = Mockito.mock(MinuteCandleStore::class.java)
        Mockito.`when`(store.upsert(Mockito.anyList())).thenAnswer {
            upsertStarted.countDown()
            assertTrue(allowCommit.await(5, TimeUnit.SECONDS))
            1
        }
        val fetcher = MinuteCandleFetcher { _, _, _ ->
            calls.incrementAndGet()
            KisMinuteChart(10, listOf(KisMinuteCandle(code, "20260804", "0800", 1, 1, 1, 1, 10, 10)))
        }

        val follower = RedisMinuteRefreshLock(StringRedisTemplate(followerConnection), reader(firstRead))
        val config = PriceRedisListenerConfig()
        val executor = config.priceRedisListenerExecutor().apply { initialize() }
        val container = config.priceRedisListenerContainer(
            followerConnection, RedisDemandSource(StringRedisTemplate(followerConnection)), follower, executor,
        )
        container.afterPropertiesSet()
        val pool = Executors.newFixedThreadPool(2)
        try {
            container.start()
            val original = pool.submit<Int> { service(owner, store, fetcher).refresh(code) }
            assertTrue(upsertStarted.await(5, TimeUnit.SECONDS))
            val jobId = checkNotNull(commands.opsForValue().get(Keys.minuteRefreshLock(code)))
            val joining = pool.submit<Int> { service(follower, store, fetcher, waitMillis = 10_000).refresh(code) }
            assertTrue(firstRead.await(5, TimeUnit.SECONDS))
            assertNull(commands.opsForValue().get(Keys.minuteRefreshResult(jobId)))
            assertFalse(joining.isDone)

            allowCommit.countDown()

            assertEquals(1, original.get(3, TimeUnit.SECONDS))
            assertEquals(1, joining.get(3, TimeUnit.SECONDS))
            assertEquals(1, calls.get())
            assertEquals("ok:1", commands.opsForValue().get(Keys.minuteRefreshResult(jobId)))
            assertTrue(commands.getExpire(Keys.minuteRefreshResult(jobId)) > 0)
        } finally {
            allowCommit.countDown()
            pool.shutdownNow()
            container.destroy()
            executor.shutdown()
        }
    }

    @Test
    fun `후속 워커는 락만 남은 작업에서 제한 시간 후 반환하고 수집하지 않는다`() {
        val claim = owner.claim(code, Duration.ofSeconds(90))
        val follower = RedisMinuteRefreshLock(
            StringRedisTemplate(followerConnection), ReactiveStringRedisTemplate(followerConnection),
        )
        val store = Mockito.mock(MinuteCandleStore::class.java)
        val fetcher = MinuteCandleFetcher { _, _, _ -> error("joined request started KIS collection") }

        assertEquals(0, service(follower, store, fetcher).refresh(code))
        assertEquals(claim.jobId, commands.opsForValue().get(Keys.minuteRefreshLock(code)))
        Mockito.verifyNoInteractions(store)
    }

    @Test
    fun `다음 작업은 이전 작업의 결과나 이전 소유자의 해제에 영향받지 않는다`() {
        val first = owner.claim(code, Duration.ofSeconds(90))
        val follower = RedisMinuteRefreshLock(
            StringRedisTemplate(followerConnection), ReactiveStringRedisTemplate(followerConnection),
        )
        val joined = follower.claim(code, Duration.ofSeconds(90))
        assertFalse(joined.acquired)
        assertEquals(first.jobId, joined.jobId)
        owner.complete(first.jobId, Result.Succeeded(7))
        owner.release(code, first.jobId)
        val second = owner.claim(code, Duration.ofSeconds(90))
        assertNotEquals(first.jobId, second.jobId)

        owner.release(code, first.jobId)

        assertEquals(second.jobId, commands.opsForValue().get(Keys.minuteRefreshLock(code)))
        assertEquals(Result.Succeeded(7), follower.awaitResult(joined.jobId, Duration.ofSeconds(2)))
        assertNull(follower.awaitResult(second.jobId, Duration.ofMillis(300)))
    }

    private fun reader(firstRead: CountDownLatch): ReactiveStringRedisTemplate {
        val actual = ReactiveStringRedisTemplate(followerConnection)
        val template = Mockito.mock(ReactiveStringRedisTemplate::class.java)
        @Suppress("UNCHECKED_CAST")
        val ops = Mockito.mock(ReactiveValueOperations::class.java) as ReactiveValueOperations<String, String>
        Mockito.`when`(template.opsForValue()).thenReturn(ops)
        Mockito.`when`(ops.get(Mockito.anyString())).thenAnswer {
            actual.opsForValue().get(it.getArgument<String>(0)).doOnSuccess { firstRead.countDown() }
        }
        return template
    }

    private fun service(
        lock: MinuteRefreshLock,
        store: MinuteCandleStore,
        fetcher: MinuteCandleFetcher,
        waitMillis: Long = 500,
    ) = MinuteCandleRefreshService(
        fetcher = fetcher,
        store = store,
        calendar = MarketCalendar(enforced = true, clock = now::toInstant),
        refreshLock = lock,
        watermarks = Mockito.mock(MinuteRefreshWatermarkStore::class.java),
        marketDivs = Mockito.mock(MinuteMarketDivStore::class.java),
        knownDivs = InMemoryMarketDivStore(),
        freshSeconds = 60,
        meters = SimpleMeterRegistry(),
        waitTimeoutMillis = waitMillis,
        now = { now },
    )
}
