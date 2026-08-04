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
import kotlin.test.assertNull
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

        override fun purgeBatchBefore(dateExclusive: String, batchSize: Int): Int {
            val victims = rows.keys.filter { it.second < dateExclusive }.take(batchSize)
            victims.forEach(rows::remove)
            return victims.size
        }
    }

    private inner class PagingFetcher(
        private val date: String = "20260804",
        private val accPerMinute: Long = 100,
        private val firstBar: LocalTime = LocalTime.of(8, 0),
        private val lastBar: LocalTime = LocalTime.of(20, 0),
    ) : MinuteCandleFetcher {
        val calls = AtomicInteger()

        override fun fetch(code: String, to: LocalTime): List<KisMinuteCandle> {
            calls.incrementAndGet()
            val end = minOf(to.withSecond(0).withNano(0), lastBar)
            return generateSequence(end) { it.minusMinutes(1) }
                .takeWhile { it >= firstBar }
                .take(30)
                .map { time ->
                    val barIndex = (time.toSecondOfDay() - firstBar.toSecondOfDay()) / 60 + 1
                    KisMinuteCandle(
                        code = code,
                        date = date,
                        time = time.format(hhmm),
                        open = 230000,
                        high = 230500,
                        low = 229500,
                        close = 230200,
                        volume = 1000,
                        accValue = barIndex * accPerMinute,
                    )
                }
                .toList()
        }
    }

    private class InMemoryWatermarks : MinuteRefreshWatermarkStore {
        private val marks = ConcurrentHashMap<String, String>()

        override fun fetchedThrough(code: String, date: String): String? = marks["$code:$date"]

        override fun record(code: String, date: String, time: String) {
            marks.merge("$code:$date", time) { old, new -> maxOf(old, new) }
        }
    }

    private class FakeRefreshLock(private val acquirable: Boolean = true) : MinuteRefreshLock {
        val acquired = AtomicInteger()
        val released = AtomicInteger()

        override fun tryAcquire(code: String, ttl: java.time.Duration): Boolean {
            if (acquirable) acquired.incrementAndGet()
            return acquirable
        }

        override fun release(code: String) {
            released.incrementAndGet()
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
        fetchDeadlineMillis: Long = 10_000,
        lock: MinuteRefreshLock = FakeRefreshLock(),
        watermarks: MinuteRefreshWatermarkStore = InMemoryWatermarks(),
    ) = MinuteCandleRefreshService(
        fetcher = fetcher,
        store = store,
        calendar = calendar(at),
        refreshLock = lock,
        watermarks = watermarks,
        freshSeconds = freshSeconds,
        meters = SimpleMeterRegistry(),
        waitTimeoutMillis = waitTimeoutMillis,
        fetchDeadlineMillis = fetchDeadlineMillis,
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
    fun `저장된 마지막 분 다음부터 완결 분까지 전방 페이징한다`() {
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
        assertEquals("1303", store.latestTime("005930", "20260804"))
    }

    @Test
    fun `누적 거래대금은 저장분 합계를 앵커로 분당 값으로 변환한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher(accPerMinute = 100)
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 9, 30, 30, 0, seoul))

        service.refresh("005930")

        val bar0929 = store.rows.getValue(Triple("005930", "20260804", "0929"))
        assertEquals(100, bar0929.value)
        val bar0900 = store.rows.getValue(Triple("005930", "20260804", "0900"))
        assertEquals(100, bar0900.value)
        assertNull(store.rows[Triple("005930", "20260804", "0930")])
    }

    @Test
    fun `거래 재개가 늦은 종목은 빈 창을 건너뛰며 앞으로 나아간다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher(firstBar = LocalTime.of(11, 0))
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 11, 35, 30, 0, seoul))

        val synced = service.refresh("005930")

        assertEquals(35, synced)
        assertEquals("1134", store.latestTime("005930", "20260804"))
        assertEquals(100, store.rows.getValue(Triple("005930", "20260804", "1100")).value)
    }

    @Test
    fun `당일 마감봉이 있으면 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        store.upsert(listOf(MinuteCandle("005930", "20260804", "2000", 1, 1, 1, 1, 1, 1)))
        val fetcher = PagingFetcher()
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 20, 30, 0, 0, seoul))

        assertEquals(0, service.refresh("005930"))
        assertEquals(0, fetcher.calls.get())
    }

    @Test
    fun `휴장일과 개장 전에는 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val saturday = ZonedDateTime.of(2026, 8, 1, 13, 0, 0, 0, seoul)
        assertEquals(0, service(fetcher, store, at = saturday).refresh("005930"))
        val beforeOpen = ZonedDateTime.of(2026, 8, 4, 7, 30, 0, 0, seoul)
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
    fun `분산 락을 얻지 못하면 다른 인스턴스가 신선화 중이므로 KIS를 건너뛴다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val service = service(fetcher, store, lock = FakeRefreshLock(acquirable = false))

        assertEquals(0, service.refresh("005930"))
        assertEquals(0, fetcher.calls.get())
    }

    @Test
    fun `신선화가 끝나면 분산 락을 해제한다`() {
        val store = InMemoryMinuteStore()
        val lock = FakeRefreshLock()
        service(PagingFetcher(), store, lock = lock).refresh("005930")

        assertEquals(1, lock.acquired.get())
        assertEquals(1, lock.released.get())
    }

    @Test
    fun `페치 데드라인이 지나도 저장분과 연속된 구간만 적재해 중간 공백이 생기지 않는다`() {
        val store = InMemoryMinuteStore()
        store.upsert(
            (0..180).map { offset ->
                val time = LocalTime.of(9, 0).plusMinutes(offset.toLong())
                MinuteCandle("005930", "20260804", time.format(hhmm), 1, 1, 1, 1, 1, 100)
            },
        )
        val fetcher = PagingFetcher()
        val service = service(fetcher, store, fetchDeadlineMillis = 0, freshSeconds = 0)

        assertEquals(30, service.refresh("005930"))
        assertEquals(1, fetcher.calls.get())
        assertEquals("1230", store.latestTime("005930", "20260804"))

        assertEquals(30, service.refresh("005930"))
        assertEquals("1300", store.latestTime("005930", "20260804"))
        assertEquals(181 + 60, store.rows.size)
    }

    @Test
    fun `syncDay는 신선 임계를 무시하고 조회 데드라인보다 긴 예산으로 완주한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val service = MinuteCandleRefreshService(
            fetcher = fetcher,
            store = store,
            calendar = calendar(),
            refreshLock = FakeRefreshLock(),
            watermarks = InMemoryWatermarks(),
            freshSeconds = 60,
            meters = SimpleMeterRegistry(),
            fetchDeadlineMillis = 0,
            dailySyncDeadlineMillis = 120_000,
            now = { tradingNow },
        )

        assertEquals(30, service.refresh("005930"))
        assertEquals("0829", store.latestTime("005930", "20260804"))

        val synced = service.syncDay("005930")

        assertEquals("1303", store.latestTime("005930", "20260804"))
        assertEquals(274, synced)
    }

    @Test
    fun `마지막 체결이 이른 종목도 마감까지 조회를 마치면 워터마크로 완주다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher(lastBar = LocalTime.of(14, 0))
        val afterClose = ZonedDateTime.of(2026, 8, 4, 20, 10, 0, 0, seoul)
        val service = service(fetcher, store, at = afterClose)

        service.syncDay("005930")

        assertEquals("1400", store.latestTime("005930", "20260804"))
        assertTrue(service.isDayComplete("005930", "20260804"))

        val callsAfterFirst = fetcher.calls.get()
        assertEquals(0, service.syncDay("005930"))
        assertEquals(callsAfterFirst, fetcher.calls.get())
    }

    @Test
    fun `데드라인에 잘린 조회는 워터마크를 남기지 않아 완주로 오판되지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val afterClose = ZonedDateTime.of(2026, 8, 4, 20, 10, 0, 0, seoul)
        val service = service(fetcher, store, at = afterClose, fetchDeadlineMillis = 0, freshSeconds = 0)

        service.refresh("005930")

        assertEquals("0829", store.latestTime("005930", "20260804"))
        assertEquals(false, service.isDayComplete("005930", "20260804"))
    }

    @Test
    fun `장 마감 뒤 콜드 조회는 25콜 이내로 08시부터 20시까지 채우고 완주한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher()
        val afterClose = ZonedDateTime.of(2026, 8, 4, 20, 1, 0, 0, seoul)
        val service = service(fetcher, store, at = afterClose)

        val synced = service.syncDay("005930")

        assertEquals(721, synced)
        assertEquals("0800", store.rows.keys.minOf { it.third })
        assertEquals("2000", store.latestTime("005930", "20260804"))
        assertTrue(fetcher.calls.get() <= 25, "콜 수=${fetcher.calls.get()}")
        assertTrue(service.isDayComplete("005930", "20260804"))
    }

    @Test
    fun `워터마크는 인스턴스 간에 공유되어 다른 인스턴스도 완주로 본다`() {
        val store = InMemoryMinuteStore()
        val shared = InMemoryWatermarks()
        val afterClose = ZonedDateTime.of(2026, 8, 4, 20, 10, 0, 0, seoul)
        val instanceA = service(PagingFetcher(lastBar = LocalTime.of(14, 0)), store, at = afterClose, watermarks = shared)
        val fetcherB = PagingFetcher(lastBar = LocalTime.of(14, 0))
        val instanceB = service(fetcherB, store, at = afterClose, watermarks = shared)

        instanceA.syncDay("005930")

        assertTrue(instanceB.isDayComplete("005930", "20260804"))
        assertEquals(0, instanceB.syncDay("005930"))
        assertEquals(0, fetcherB.calls.get())
    }

    @Test
    fun `syncDay는 진행 중인 refresh 결과를 자기 결과로 가로채지 않는다`() {
        val store = InMemoryMinuteStore()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val fetcher = MinuteCandleFetcher { code, to ->
            entered.countDown()
            release.await(5, TimeUnit.SECONDS)
            PagingFetcher().fetch(code, to)
        }
        val service = service(fetcher, store, waitTimeoutMillis = 200)
        val pool = Executors.newFixedThreadPool(2)
        try {
            val refreshing = pool.submit<Int> { service.refresh("005930") }
            assertTrue(entered.await(5, TimeUnit.SECONDS))
            val syncing = pool.submit<Int> { service.syncDay("005930") }
            Thread.sleep(100)
            release.countDown()

            val refreshed = refreshing.get(5, TimeUnit.SECONDS)
            val synced = syncing.get(5, TimeUnit.SECONDS)

            assertTrue(refreshed > 0)
            assertEquals(0, synced)
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `수집 창 밖 봉이 섞여 와도 저장하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = PagingFetcher(firstBar = LocalTime.of(7, 30))
        val service = service(fetcher, store, at = ZonedDateTime.of(2026, 8, 4, 8, 20, 30, 0, seoul))

        service.refresh("005930")

        assertTrue(store.rows.keys.all { it.third >= "0800" })
        assertEquals("0800", store.rows.keys.minOf { it.third })
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
