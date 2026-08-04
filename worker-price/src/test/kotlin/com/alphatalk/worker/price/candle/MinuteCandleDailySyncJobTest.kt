package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.calendar.MarketCalendar
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import kotlin.test.Test
import kotlin.test.assertEquals

class MinuteCandleDailySyncJobTest {
    private val date = "20260804"

    private class StubMinuteStore(
        private val latestByCode: MutableMap<String, String> = ConcurrentHashMap(),
        private val codes: Set<String> = emptySet(),
    ) : MinuteCandleStore {
        fun setLatest(code: String, time: String) {
            latestByCode[code] = time
        }

        override fun upsert(candles: List<MinuteCandle>): Int = candles.size
        override fun latestTime(code: String, date: String): String? = latestByCode[code]
        override fun sumValueBefore(code: String, date: String, timeExclusive: String): Long = 0
        override fun codesOn(date: String): Set<String> = codes
        override fun purgeBefore(dateExclusive: String): Int = 0
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
        leader = object : com.alphatalk.worker.price.leader.LeaderLock {
            override fun tryAcquire() = true
            override fun release() {}
        },
        today = { LocalDate.of(2026, 8, 4) },
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

        assertEquals(1, job.syncOnce())
        assertEquals(listOf("005930"), calls)
    }

    @Test
    fun `부분 적재로 미완주면 같은 날 즉시 재시도해 완주시킨다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            if (calls.size == 1) store.setLatest(code, "1230") else store.setLatest(code, "1530")
            1
        }

        assertEquals(1, job.syncOnce())
        assertEquals(2, calls.size)
        assertEquals("1530", store.latestTime("005930", date))
    }

    @Test
    fun `재시도에도 미완주면 성공으로 세지 않는다`() {
        val store = StubMinuteStore()
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            store.setLatest(code, "1230")
            1
        }

        assertEquals(0, job.syncOnce())
        assertEquals(2, calls.size)
    }

    @Test
    fun `이미 완주한 종목은 KIS 동기화를 건너뛴다`() {
        val store = StubMinuteStore()
        store.setLatest("005930", "1530")
        val calls = mutableListOf<String>()
        val job = job(store, setOf("005930")) { code ->
            calls += code
            0
        }

        assertEquals(1, job.syncOnce())
        assertEquals(0, calls.size)
    }
}
