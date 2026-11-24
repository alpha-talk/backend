package com.alphatalk.coreapi.stockinfo

import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class HttpMinuteCandleRefresherTest {
    private lateinit var server: HttpServer
    private val serverExecutor = Executors.newFixedThreadPool(4)
    private val hits = AtomicInteger()
    private val gate = CountDownLatch(1)
    private val requestStarted = CountDownLatch(1)
    private var blocking = false

    @BeforeTest
    fun setUp() {
        server = HttpServer.create(InetSocketAddress(0), 0)
        server.executor = serverExecutor
        server.createContext("/internal/minute-candles") { exchange ->
            hits.incrementAndGet()
            requestStarted.countDown()
            if (blocking) gate.await(5, TimeUnit.SECONDS)
            exchange.sendResponseHeaders(200, -1)
            exchange.close()
        }
        server.start()
    }

    @AfterTest
    fun tearDown() {
        gate.countDown()
        if (::server.isInitialized) server.stop(0)
        serverExecutor.shutdownNow()
    }

    private fun baseUrl() = "http://localhost:${server.address.port}"

    private fun refresher(clock: AtomicLong, throttleMillis: Long = 1_000) = HttpMinuteCandleRefresher(
        baseUrl = baseUrl(),
        clock = { clock.get() },
        throttleMillis = throttleMillis,
    )

    @Test
    fun `스로틀 창 안의 재요청은 워커를 다시 때리지 않는다`() {
        val clock = AtomicLong(1_000_000)
        val refresher = refresher(clock)

        refresher.refresh("005930")
        refresher.refresh("005930")
        refresher.refresh("005930")

        assertEquals(1, hits.get())
    }

    @Test
    fun `스로틀 창이 지나면 다시 트리거한다`() {
        val clock = AtomicLong(1_000_000)
        val refresher = refresher(clock)

        refresher.refresh("005930")
        clock.addAndGet(1_500)
        refresher.refresh("005930")

        assertEquals(2, hits.get())
    }

    @Test
    fun `종목이 다르면 스로틀도 분리된다`() {
        val clock = AtomicLong(1_000_000)
        val refresher = refresher(clock)

        refresher.refresh("005930")
        refresher.refresh("000660")

        assertEquals(2, hits.get())
    }

    @Test
    fun `같은 종목 동시 요청은 한 번만 워커를 때린다`() {
        blocking = true
        val clock = AtomicLong(1_000_000)
        val refresher = refresher(clock)
        val pool = Executors.newFixedThreadPool(8)
        try {
            val tasks = (1..8).map { pool.submit { refresher.refresh("005930") } }
            Thread.sleep(200)
            gate.countDown()
            tasks.forEach { it.get(5, TimeUnit.SECONDS) }
        } finally {
            pool.shutdownNow()
        }

        assertEquals(1, hits.get())
    }

    @Test
    fun `읽기와 같은 종목 합류는 기본 1초를 넘겨 워커의 응답을 기다린다`() {
        blocking = true
        val refresher = refresher(AtomicLong(1_000_000))
        val pool = Executors.newFixedThreadPool(2)
        try {
            val leader = pool.submit { refresher.refresh("005930") }
            assertTrue(requestStarted.await(1, TimeUnit.SECONDS))
            val joining = CountDownLatch(1)
            val follower = pool.submit {
                joining.countDown()
                refresher.refresh("005930")
            }
            assertTrue(joining.await(1, TimeUnit.SECONDS))

            assertFailsWith<TimeoutException> { follower.get(1_200, TimeUnit.MILLISECONDS) }
            assertTrue(!leader.isDone)
            gate.countDown()

            leader.get(2, TimeUnit.SECONDS)
            follower.get(2, TimeUnit.SECONDS)
            assertEquals(1, hits.get())
        } finally {
            gate.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `워커가 죽어 있어도 예외를 던지지 않는다`() {
        val dead = HttpMinuteCandleRefresher(
            baseUrl = "http://localhost:1",
            connectTimeout = Duration.ofMillis(100),
            readTimeout = Duration.ofMillis(100),
        )

        dead.refresh("005930")

        assertTrue(true)
    }
}
