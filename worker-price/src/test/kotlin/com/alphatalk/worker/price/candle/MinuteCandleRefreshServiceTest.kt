package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteCandle
import com.alphatalk.worker.price.calendar.MarketCalendar
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinuteCandleRefreshServiceTest {
    private val seoul = ZoneId.of("Asia/Seoul")
    private val hhmm = DateTimeFormatter.ofPattern("HHmm")
    private val tradingNow: ZonedDateTime = ZonedDateTime.of(2026, 8, 4, 13, 4, 30, 0, seoul)

    private class InMemoryMinuteStore : MinuteCandleStore {
        val rows = ConcurrentHashMap<Triple<String, String, String>, MinuteCandle>()

        override fun upsert(candles: List<MinuteCandle>): Int {
            candles.forEach { rows[Triple(it.code, it.date, it.time)] = it }
            return candles.size
        }

        override fun latestTime(code: String, date: String): String? =
            rows.keys.filter { it.first == code && it.second == date }.maxOfOrNull { it.third }

        override fun sumValueBefore(code: String, date: String, timeExclusive: String): Long =
            rows.values.filter { it.code == code && it.date == date && it.time < timeExclusive }.sumOf { it.value }

        override fun codesOn(date: String): Set<String> =
            rows.keys.filter { it.second == date }.map { it.first }.toSet()

        override fun purgeBefore(dateExclusive: String): Int {
            val victims = rows.keys.filter { it.second < dateExclusive }
            victims.forEach(rows::remove)
            return victims.size
        }
    }

    private inner class PagingFetcher(
        private val date: String = "20260804",
        private val accPerMinute: Long = 100,
    ) : MinuteCandleFetcher {
        val calls = AtomicInteger()

        override fun fetch(code: String, to: LocalTime): List<KisMinuteCandle> {
            calls.incrementAndGet()
            val open = LocalTime.of(9, 0)
            val end = to.withSecond(0).withNano(0)
            return generateSequence(end) { it.minusMinutes(1) }
                .takeWhile { it >= open }
                .take(30)
                .map { time ->
                    val minutesFromOpen = (time.toSecondOfDay() - open.toSecondOfDay()) / 60 + 1
                    KisMinuteCandle(
                        code = code,
                        date = date,
                        time = time.format(hhmm),
                        open = 230000,
                        high = 230500,
                        low = 229500,
                        close = 230200,
                        volume = 1000,
                        accValue = minutesFromOpen * accPerMinute,
                    )
                }
                .toList()
        }
    }

    private fun calendar(at: ZonedDateTime = tradingNow) =
        MarketCalendar(enforced = true, clock = { at.toInstant() })

    private fun service(
        fetcher: MinuteCandleFetcher,
        store: MinuteCandleStore,
        at: ZonedDateTime = tradingNow,
        freshSeconds: Long = 60,
        waitTimeoutMillis: Long = 2_000,
    ) = MinuteCandleRefreshService(
        fetcher = fetcher,
        store = store,
        calendar = calendar(at),
        freshSeconds = freshSeconds,
        meters = SimpleMeterRegistry(),
        waitTimeoutMillis = waitTimeoutMillis,
        now = { at },
    )

    @Test
    fun `동시 요청은 single-flight로 합쳐져 KIS 조회가 한 번만 나간다`() {
        val store = InMemoryMinuteStore()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val calls = AtomicInteger()
        val fetcher = MinuteCandleFetcher { code, to ->
            calls.incrementAndGet()
            entered.countDown()
            release.await(5, TimeUnit.SECONDS)
            PagingFetcher().fetch(code, to)
        }
        val service = service(fetcher, store)
        val pool = Executors.newFixedThreadPool(8)
        try {
            val first = pool.submit<Int> { service.refresh("005930") }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val others = (1..7).map { pool.submit<Int> { service.refresh("005930") } }
            Thread.sleep(200)
            release.countDown()
            val firstResult = first.get(5, TimeUnit.SECONDS)
            val otherResults = others.map { it.get(5, TimeUnit.SECONDS) }
            assertTrue(firstResult > 0)
            assertTrue(otherResults.all { it == firstResult || it == 0 })
        } finally {
            pool.shutdownNow()
        }
        assertEquals(expectedPages(store), calls.get())
    }

    private fun expectedPages(store: InMemoryMinuteStore): Int = (store.rows.size + 29) / 30

    @Test
    fun `신선 임계 안의 재요청은 KIS를 다시 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val service = service(fetcher, store)

        service.refresh("005930")
        val after = fetcher.calls.get()
        service.refresh("005930")

        assertEquals(after, fetcher.calls.get())
    }

    @Test
    fun `저장된 마지막 분 이후 공백만큼만 역방향 페이징한다`() {
        val store = InMemoryMinuteStore()
        store.upsert(
            (0..180).map { offset ->
                val time = LocalTime.of(9, 0).plusMinutes(offset.toLong())
                MinuteCandle("005930", "20260804", time.format(hhmm), 1, 1, 1, 1, 1, 100)
            },
        )
        val fetcher = PagingFetcher()
        val service = service(fetcher, store)

        service.refresh("005930")

        assertEquals(3, fetcher.calls.get())
        assertEquals("1304", store.latestTime("005930", "20260804"))
    }

    @Test
    fun `누적 거래대금은 저장분 합계를 앵커로 분당 값으로 변환한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher(accPerMinute = 100)
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 9, 30, 30, 0, seoul))

        service.refresh("005930")

        val bar0930 = store.rows.getValue(Triple("005930", "20260804", "0930"))
        assertEquals(100, bar0930.value)
        val bar0900 = store.rows.getValue(Triple("005930", "20260804", "0900"))
        assertEquals(100, bar0900.value)
    }

    @Test
    fun `당일 마감봉이 있으면 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        store.upsert(listOf(MinuteCandle("005930", "20260804", "1530", 1, 1, 1, 1, 1, 1)))
        val fetcher = PagingFetcher()
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 16, 30, 0, 0, seoul))

        assertEquals(0, service.refresh("005930"))
        assertEquals(0, fetcher.calls.get())
    }

    @Test
    fun `휴장일과 개장 전에는 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val saturday = ZonedDateTime.of(2026, 8, 1, 13, 0, 0, 0, seoul)
        assertEquals(0, service(fetcher, store, at = saturday).refresh("005930"))
        val beforeOpen = ZonedDateTime.of(2026, 8, 4, 8, 30, 0, 0, seoul)
        assertEquals(0, service(fetcher, store, at = beforeOpen).refresh("005930"))
        assertEquals(0, fetcher.calls.get())
    }

    @Test
    fun `대기자는 타임아웃을 넘기면 0으로 빠져 요청 스레드가 쌓이지 않는다`() {
        val store = InMemoryMinuteStore()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val fetcher = MinuteCandleFetcher { code, to ->
            entered.countDown()
            release.await(10, TimeUnit.SECONDS)
            PagingFetcher().fetch(code, to)
        }
        val service = service(fetcher, store, waitTimeoutMillis = 100)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val winner = pool.submit<Int> { service.refresh("005930") }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val waiter = pool.submit<Int> { service.refresh("005930") }
            assertEquals(0, waiter.get(2, TimeUnit.SECONDS))
            release.countDown()
            assertTrue(winner.get(5, TimeUnit.SECONDS) > 0)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `실패한 요청 뒤의 재요청은 다시 조회한다`() {
        val store = InMemoryMinuteStore()
        val calls = AtomicInteger()
        val fetcher = MinuteCandleFetcher { code, to ->
            if (calls.incrementAndGet() == 1) throw IllegalStateException("kis down")
            PagingFetcher().fetch(code, to)
        }
        val service = service(fetcher, store)

        runCatching { service.refresh("005930") }
        val synced = service.refresh("005930")

        assertTrue(synced > 0)
    }
}
