package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.leader.LeaderLock
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CandleSyncJobTest {
    private val today = LocalDate.of(2026, 7, 24)

    private fun candle(code: String, date: String) = KisDailyCandle(
        code = code,
        date = date,
        open = 70600,
        high = 71500,
        low = 70400,
        close = 71200,
        volume = 1234567,
        value = 87942671300,
    )

    private class RecordingFetcher(
        private val respond: (String) -> List<KisDailyCandle>,
    ) : DailyCandleFetcher {
        val requested = mutableListOf<Triple<String, LocalDate, LocalDate>>()

        override fun fetch(code: String, from: LocalDate, to: LocalDate): List<KisDailyCandle> {
            requested += Triple(code, from, to)
            return respond(code)
        }
    }

    private class InMemoryCandleStore : DailyCandleStore {
        val rows = mutableMapOf<Pair<String, String>, KisDailyCandle>()

        override fun upsert(candles: List<KisDailyCandle>): Int {
            candles.forEach { rows[it.code to it.date] = it }
            return candles.size
        }

        override fun latestDate(code: String): String? =
            rows.keys.filter { it.first == code }.maxOfOrNull { it.second }
    }

    private class ToggleLeaderLock(var leader: Boolean) : LeaderLock {
        override fun tryAcquire(): Boolean = leader

        override fun release() {
        }
    }

    private fun calendarAt(day: Int): MarketCalendar {
        val clock = { ZonedDateTime.of(2026, 7, day, 16, 30, 0, 0, ZoneId.of("Asia/Seoul")).toInstant() }
        return MarketCalendar(clock = clock)
    }

    private fun job(
        fetcher: DailyCandleFetcher,
        store: DailyCandleStore,
        symbols: Set<String> = setOf("005930"),
        calendar: MarketCalendar = MarketCalendar(enforced = false, clock = Instant::now),
        leader: LeaderLock = ToggleLeaderLock(leader = true),
    ) = job(fetcher, store, { symbols }, calendar, leader)

    private fun job(
        fetcher: DailyCandleFetcher,
        store: DailyCandleStore,
        symbols: () -> Set<String>,
        calendar: MarketCalendar = MarketCalendar(enforced = false, clock = Instant::now),
        leader: LeaderLock = ToggleLeaderLock(leader = true),
    ) = CandleSyncJob(
        symbols = symbols,
        fetcher = fetcher,
        store = store,
        backfillDays = 90,
        calendar = calendar,
        leader = leader,
        meters = SimpleMeterRegistry(),
        today = { today },
    )

    @Test
    fun `첫 동기화는 백필 구간부터 조회해 적재한다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260723"), candle(code, "20260724")) }
        val store = InMemoryCandleStore()

        val synced = job(fetcher, store).syncOnce().synced

        assertEquals(1, synced)
        assertEquals(Triple("005930", today.minusDays(90), today), fetcher.requested.single())
        assertEquals(2, store.rows.size)
    }

    @Test
    fun `이후 동기화는 마지막 일자 다음날부터 조회한다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()
        store.upsert(listOf(candle("005930", "20260723")))

        job(fetcher, store).syncOnce()

        assertEquals(LocalDate.of(2026, 7, 24), fetcher.requested.single().second)
    }

    @Test
    fun `이미 최신이면 조회하지 않는다`() {
        val fetcher = RecordingFetcher { emptyList() }
        val store = InMemoryCandleStore()
        store.upsert(listOf(candle("005930", "20260724")))

        val synced = job(fetcher, store).syncOnce().synced

        assertEquals(0, synced)
        assertTrue(fetcher.requested.isEmpty())
    }

    @Test
    fun `한 종목이 실패해도 나머지는 동기화된다`() {
        val fetcher = RecordingFetcher { code ->
            if (code == "005930") throw IllegalStateException("boom")
            listOf(candle(code, "20260724"))
        }
        val store = InMemoryCandleStore()

        val result = job(fetcher, store, symbols = linkedSetOf("005930", "000660")).syncOnce()

        assertEquals(1, result.synced)
        assertEquals(1, result.failed)
        assertFalse(result.complete)
        assertEquals(setOf("000660" to "20260724"), store.rows.keys)
    }

    @Test
    fun `거래일의 리더는 예약 동기화를 수행한다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()

        job(fetcher, store, calendar = calendarAt(27)).syncDaily()

        assertEquals(1, fetcher.requested.size)
    }

    @Test
    fun `휴장일에는 예약 동기화를 건너뛴다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()

        job(fetcher, store, calendar = calendarAt(26)).syncDaily()

        assertTrue(fetcher.requested.isEmpty())
    }

    @Test
    fun `스탠바이 인스턴스는 예약 동기화를 건너뛴다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()

        job(fetcher, store, calendar = calendarAt(27), leader = ToggleLeaderLock(leader = false)).syncDaily()

        assertTrue(fetcher.requested.isEmpty())
    }

    @Test
    fun `종목 실패가 있으면 회차를 미완료로 두고 후속 회차가 재시도한다`() {
        val store = InMemoryCandleStore()
        var attempt = 0
        val fetcher = RecordingFetcher { code ->
            if (code == "005930" && attempt == 0) throw IllegalStateException("boom")
            listOf(candle(code, "20260724"))
        }

        val job = job(fetcher, store, symbols = linkedSetOf("005930", "000660"), calendar = calendarAt(27))
        job.syncDaily()
        attempt = 1

        job.retryUnfinished()

        assertEquals(setOf("005930" to "20260724", "000660" to "20260724"), store.rows.keys)
    }

    @Test
    fun `재시도 회차는 이미 적재된 종목을 다시 조회하지 않는다`() {
        val store = InMemoryCandleStore()
        var attempt = 0
        val fetcher = RecordingFetcher { code ->
            if (code == "005930" && attempt == 0) throw IllegalStateException("boom")
            listOf(candle(code, "20260724"))
        }

        val job = job(fetcher, store, symbols = linkedSetOf("005930", "000660"), calendar = calendarAt(27))
        job.syncDaily()
        attempt = 1
        fetcher.requested.clear()

        job.retryUnfinished()

        assertEquals(listOf("005930"), fetcher.requested.map { it.first })
    }

    @Test
    fun `유니버스 조회 실패로 중단되면 후속 회차가 재시도해 완주한다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()
        var universeCalls = 0
        val flaky = {
            universeCalls += 1
            if (universeCalls == 1) throw IllegalStateException("db down") else setOf("005930")
        }

        val job = job(fetcher, store, flaky, calendar = calendarAt(27))
        job.syncDaily()
        assertTrue(fetcher.requested.isEmpty())

        job.retryUnfinished()

        assertEquals(listOf("005930"), fetcher.requested.map { it.first })
    }

    @Test
    fun `정기 회차가 완주했으면 후속 재시도는 다시 조회하지 않는다`() {
        val fetcher = RecordingFetcher { code -> listOf(candle(code, "20260724")) }
        val store = InMemoryCandleStore()

        val job = job(fetcher, store, calendar = calendarAt(27))
        job.syncDaily()
        job.retryUnfinished()

        assertEquals(1, fetcher.requested.size)
    }
}
