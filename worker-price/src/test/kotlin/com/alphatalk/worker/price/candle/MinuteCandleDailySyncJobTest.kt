package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.leader.LeaderLock
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinuteCandleDailySyncJobTest {
    private val date = "20260804"

    private class StubMinuteStore : MinuteCandleStore {
        private val latestByCode: MutableMap<String, String> = ConcurrentHashMap()

        fun setLatest(code: String, time: String) {
            latestByCode[code] = time
        }

        override fun upsert(candles: List<MinuteCandle>): Int = candles.size
        override fun latestTime(code: String, date: String): String? = latestByCode[code]
        override fun sumValueBefore(code: String, date: String, timeExclusive: String): Long = 0
        override fun codesOn(date: String): Set<String> = latestByCode.keys.toSet()
        override fun purgeBatchBefore(dateExclusive: String, batchSize: Int): Int = 0
    }

    private class AlwaysLeader : LeaderLock {
        override fun tryAcquire() = true
        override fun release() {}
    }

    private fun job(
        store: StubMinuteStore,
        symbols: Set<String>,
        syncDay: (String) -> Int,
    ) = MinuteCandleDailySyncJob(
        symbols = { symbols },
        store = store,
        syncDay = syncDay,
        isDayComplete = { code, d -> (store.latestTime(code, d) ?: "") >= "1530" },
        calendar = MarketCalendar(enforced = false),
        leader = AlwaysLeader(),
        today = { LocalDate.of(2026, 8, 4) },
        sleeper = { },
    )

    @Test
    fun `완주한 종목은 한 번만 동기화하고 성공으로 센다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            store.setLatest(code, "1530")
            391
        }

        val result = job.syncOnce()

        assertEquals(1, result.completed)
        assertTrue(result.complete)
        assertEquals(listOf("005930"), calls)
    }

    @Test
    fun `부분 적재로 미완주면 같은 회차에서 즉시 재시도해 완주시킨다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            if (calls.size == 1) store.setLatest(code, "1230") else store.setLatest(code, "1530")
            1
        }

        val result = job.syncOnce()

        assertEquals(1, result.completed)
        assertEquals(2, calls.size)
    }

    @Test
    fun `첫 시도가 예외로 죽어도 같은 회차에서 재시도한다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            if (calls.size == 1) throw IllegalStateException("kis timeout")
            store.setLatest(code, "1530")
            391
        }

        val result = job.syncOnce()

        assertEquals(1, result.completed)
        assertEquals(2, calls.size)
    }

    @Test
    fun `경합으로 진전이 없으면 시도 상한까지 반복하고 미완주로 센다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            0
        }

        val result = job.syncOnce()

        assertEquals(0, result.completed)
        assertEquals(1, result.incomplete)
        assertEquals(3, calls.size)
    }

    @Test
    fun `정기 회차가 완주하면 재시도 회차는 동기화를 건너뛴다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            store.setLatest(code, "1530")
            391
        }

        job.syncDaily()
        assertEquals(1, calls.size)
        job.retryUnfinished()
        assertEquals(1, calls.size)
    }

    @Test
    fun `정기 회차가 미완주면 재시도 회차가 다시 동기화한다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            if (calls.size >= 4) store.setLatest(code, "1530")
            1
        }

        job.syncDaily()
        assertEquals(3, calls.size)
        job.retryUnfinished()
        assertEquals(4, calls.size)
    }

    @Test
    fun `정기 회차 뒤 새로 조회된 미완주 종목도 재시도 회차가 잡는다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, emptySet()) { code ->
            calls += code
            store.setLatest(code, "1530")
            120
        }

        job.syncDaily()
        assertEquals(0, calls.size)

        store.setLatest("000660", "1230")

        job.retryUnfinished()

        assertEquals(listOf("000660"), calls)
        assertEquals("1530", store.latestTime("000660", date))
    }
}
