package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteCandle
import com.alphatalk.worker.price.calendar.MarketCalendar
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class MinuteCandleBackfillServiceTest {
    private val seoul = ZoneId.of("Asia/Seoul")
    private var currentTime: ZonedDateTime = ZonedDateTime.of(2026, 8, 12, 14, 0, 0, 0, seoul)
    private val businessDays = listOf("20260811", "20260810", "20260807", "20260806", "20260805", "20260804", "20260803")

    private class InMemoryMinuteStore : MinuteCandleStore {
        val rows = ConcurrentHashMap<Triple<String, String, String>, MinuteCandle>()
        val upsertBatches = mutableListOf<List<MinuteCandle>>()

        override fun upsert(candles: List<MinuteCandle>): Int {
            upsertBatches += candles
            candles.forEach { rows[Triple(it.code, it.date, it.time)] = it }
            return candles.size
        }

        override fun latestTime(code: String, date: String): String? =
            rows.keys.filter { it.first == code && it.second == date }.maxOfOrNull { it.third }

        override fun sumValueBefore(code: String, date: String, timeExclusive: String): Long =
            rows.values.filter { it.code == code && it.date == date && it.time < timeExclusive }.sumOf { it.value }

        override fun codesOn(date: String): Set<String> =
            rows.keys.filter { it.second == date }.map { it.first }.toSet()

        override fun purgeBatchBefore(dateExclusive: String, batchSize: Int): Int = 0
    }

    private class ScriptedFetcher(
        private val script: (date: String, to: LocalTime, div: String) -> List<KisMinuteCandle>,
    ) : DailyMinuteCandleFetcher {
        val calls = mutableListOf<Triple<String, LocalTime, String>>()

        override fun fetch(code: String, date: LocalDate, to: LocalTime, marketDiv: String): List<KisMinuteCandle> {
            val dateStr = date.format(java.time.format.DateTimeFormatter.BASIC_ISO_DATE)
            calls += Triple(dateStr, to, marketDiv)
            return script(dateStr, to, marketDiv)
        }
    }

    private class FakeBackfillLock(private val acquirable: Boolean = true) : MinuteBackfillLock {
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

    private fun bar(date: String, time: String, acc: Long, price: Long = 1000, volume: Long = 10): KisMinuteCandle =
        KisMinuteCandle(
            code = "005930",
            date = date,
            time = time,
            open = price,
            high = price,
            low = price,
            close = price,
            volume = volume,
            accValue = acc,
        )

    private fun krxFullDay(date: String): List<KisMinuteCandle> = listOf(
        bar(date, "1530", 300),
        bar(date, "0901", 200),
        bar(date, "0900", 120),
    )

    private fun service(
        fetcher: DailyMinuteCandleFetcher,
        store: MinuteCandleStore,
        lock: MinuteBackfillLock = FakeBackfillLock(),
        backfillDays: Int = 7,
        cooldownMillis: Long = 600_000,
        runBudgetMillis: Long = 120_000,
        executor: Executor = Executor { it.run() },
        nanoTime: () -> Long = System::nanoTime,
    ) = MinuteCandleBackfillService(
        fetcher = fetcher,
        store = store,
        calendar = MarketCalendar(clock = { currentTime.toInstant() }),
        backfillLock = lock,
        backfillDays = backfillDays,
        meters = SimpleMeterRegistry(),
        cooldownMillis = cooldownMillis,
        runBudgetMillis = runBudgetMillis,
        executor = executor,
        now = { currentTime },
        nanoTime = nanoTime,
    )

    @Test
    fun `직전 7영업일 중 빈 날짜만 채우고 오늘과 주말은 대상이 아니다`() {
        val store = InMemoryMinuteStore()
        store.upsert(listOf(MinuteCandle("005930", "20260811", "1530", 1, 1, 1, 1, 1, 1)))
        val fetcher = ScriptedFetcher { date, _, _ -> krxFullDay(date) + listOf(bar(before(date), "1530", 999)) }
        val service = service(fetcher, store)

        service.runBackfill("005930")

        val fetchedDates = fetcher.calls.map { it.first }.toSet()
        assertEquals(businessDays.drop(1).toSet(), fetchedDates)
        businessDays.drop(1).forEach { date ->
            assertEquals("1530", store.latestTime("005930", date))
        }
        assertTrue(fetcher.calls.none { it.first >= "20260812" })
    }

    @Test
    fun `하루 전체를 확보하면 누적 거래대금을 0 앵커로 차분해 적재한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ ->
            if (date == "20260811") krxFullDay(date) + listOf(bar("20260810", "1530", 999)) else emptyList()
        }
        val service = service(fetcher, store, backfillDays = 1)

        service.runBackfill("005930")

        val stored = store.upsertBatches.single().sortedBy { it.time }
        assertEquals(listOf(120L, 80L, 100L), stored.map { it.value })
        assertEquals(listOf("0900", "0901", "1530"), stored.map { it.time })
    }

    @Test
    fun `UN 응답에 요청일 행이 없으면 J로 폴백하고 다음 날짜는 다시 UN부터 판정한다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, div ->
            when (div) {
                "UN" -> listOf(bar("20260211", "1959", 500))
                else -> krxFullDay(date) + listOf(bar(before(date), "1530", 999))
            }
        }
        val service = service(fetcher, store, backfillDays = 2)

        service.runBackfill("005930")

        assertEquals(listOf("UN", "J", "UN", "J"), fetcher.calls.map { it.third })
        assertEquals("1530", store.latestTime("005930", "20260811"))
        assertEquals("1530", store.latestTime("005930", "20260810"))
    }

    @Test
    fun `휴장일은 UN·J 모두 요청일 행이 없어 적재 없이 다음 날짜로 넘어간다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, div ->
            when {
                date == "20260811" -> listOf(bar("20260810", "1530", 999))
                div == "J" -> krxFullDay(date) + listOf(bar(before(date), "1530", 999))
                else -> listOf(bar(before(date), "1959", 999))
            }
        }
        val service = service(fetcher, store, backfillDays = 2)

        service.runBackfill("005930")

        assertEquals(null, store.latestTime("005930", "20260811"))
        assertEquals("1530", store.latestTime("005930", "20260810"))
    }

    @Test
    fun `여러 페이지를 이어 붙여 하루를 완성하고 페이지 경계에서도 차분이 이어진다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, to, _ ->
            when {
                date != "20260811" -> emptyList()
                to == LocalTime.of(20, 0) -> listOf(bar(date, "1530", 300), bar(date, "0930", 150))
                else -> listOf(bar(date, "0901", 100), bar(date, "0900", 60), bar("20260810", "1530", 999))
            }
        }
        val service = service(fetcher, store, backfillDays = 1)

        service.runBackfill("005930")

        assertEquals(listOf(LocalTime.of(20, 0), LocalTime.of(9, 29)), fetcher.calls.map { it.second })
        val stored = store.upsertBatches.single().sortedBy { it.time }
        assertEquals(listOf("0900", "0901", "0930", "1530"), stored.map { it.time })
        assertEquals(listOf(60L, 40L, 50L, 150L), stored.map { it.value })
    }

    @Test
    fun `0봉과 수집 창 밖 행은 적재하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, div ->
            if (div == "UN") {
                listOf(bar(before(date), "1959", 999))
            } else {
                listOf(
                    bar(date, "1531", 400),
                    bar(date, "1530", 300),
                    KisMinuteCandle("005930", date, "1000", 0, 0, 0, 0, 0, 200),
                    bar(date, "0900", 120),
                    bar(before(date), "1530", 999),
                )
            }
        }
        val service = service(fetcher, store, backfillDays = 1)

        service.runBackfill("005930")

        val stored = store.upsertBatches.single().sortedBy { it.time }
        assertEquals(listOf("0900", "1530"), stored.map { it.time })
    }

    @Test
    fun `행을 모은 뒤 완주 신호 없이 빈 페이지가 오면 그날을 버린다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, to, div ->
            when {
                div == "UN" -> listOf(bar(before(date), "1959", 999))
                to == LocalTime.of(15, 30) -> listOf(bar(date, "1530", 300), bar(date, "0930", 150))
                else -> emptyList()
            }
        }
        val service = service(fetcher, store, backfillDays = 1)

        service.runBackfill("005930")

        assertEquals(listOf("UN", "J", "J"), fetcher.calls.map { it.third })
        assertTrue(store.upsertBatches.isEmpty())
    }

    @Test
    fun `페이지 상한까지 하루를 완성하지 못하면 그날은 적재하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ -> listOf(bar(date, "1000", 100)) }
        val service = service(fetcher, store, backfillDays = 1)

        service.runBackfill("005930")

        assertTrue(store.upsertBatches.isEmpty())
    }

    @Test
    fun `실행 예산이 소진되면 남은 날짜를 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ -> krxFullDay(date) }
        val service = service(fetcher, store, runBudgetMillis = 0)

        service.runBackfill("005930")

        assertTrue(fetcher.calls.isEmpty())
    }

    @Test
    fun `하루 조회 도중 예산이 소진되면 다음 페이지를 부르지 않고 그날을 버린다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ -> listOf(bar(date, "0930", 150)) }
        val ticks = java.util.concurrent.atomic.AtomicLong()
        val nanos = listOf(0L, 0L, 200_000_000L)
        val service = service(
            fetcher,
            store,
            backfillDays = 1,
            runBudgetMillis = 100,
            nanoTime = { nanos.getOrElse(ticks.getAndIncrement().toInt()) { nanos.last() } },
        )

        service.runBackfill("005930")

        assertEquals(1, fetcher.calls.size)
        assertTrue(store.upsertBatches.isEmpty())
    }

    @Test
    fun `UN 조회가 예산을 소진하면 J 폴백을 시작하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ -> listOf(bar(before(date), "1959", 999)) }
        val ticks = java.util.concurrent.atomic.AtomicLong()
        val nanos = listOf(0L, 0L, 200_000_000L)
        val service = service(
            fetcher,
            store,
            backfillDays = 1,
            runBudgetMillis = 100,
            nanoTime = { nanos.getOrElse(ticks.getAndIncrement().toInt()) { nanos.last() } },
        )

        service.runBackfill("005930")

        assertEquals(listOf("UN"), fetcher.calls.map { it.third })
        assertTrue(store.upsertBatches.isEmpty())
    }

    @Test
    fun `큐 거절은 쿨다운 클레임을 남기지 않아 다음 요청이 다시 시도한다`() {
        val attempts = AtomicInteger()
        val service = service(
            ScriptedFetcher { _, _, _ -> emptyList() },
            InMemoryMinuteStore(),
            executor = Executor {
                attempts.incrementAndGet()
                throw java.util.concurrent.RejectedExecutionException("full")
            },
        )

        service.requestAsync("005930")
        service.requestAsync("005930")

        assertEquals(2, attempts.get())
    }

    @Test
    fun `락을 못 잡으면 아무것도 조회하지 않는다`() {
        val store = InMemoryMinuteStore()
        val fetcher = ScriptedFetcher { date, _, _ -> krxFullDay(date) }
        val service = service(fetcher, store, lock = FakeBackfillLock(acquirable = false))

        service.runBackfill("005930")

        assertTrue(fetcher.calls.isEmpty())
    }

    @Test
    fun `조회 예외는 이번 실행을 멈추고 락을 풀어 다음 트리거에 맡긴다`() {
        val store = InMemoryMinuteStore()
        val lock = FakeBackfillLock()
        val fetcher = ScriptedFetcher { _, _, _ -> throw IllegalStateException("kis down") }
        val service = service(fetcher, store, lock = lock)

        service.runBackfill("005930")

        assertEquals(1, fetcher.calls.size)
        assertTrue(store.upsertBatches.isEmpty())
        assertEquals(1, lock.released.get())
    }

    @Test
    fun `쿨다운 안의 재요청은 실행을 만들지 않는다`() {
        val executed = AtomicInteger()
        val store = InMemoryMinuteStore()
        store.upsert(businessDays.map { MinuteCandle("005930", it, "1530", 1, 1, 1, 1, 1, 1) })
        val service = service(
            ScriptedFetcher { _, _, _ -> emptyList() },
            store,
            executor = Executor { executed.incrementAndGet() },
        )

        service.requestAsync("005930")
        service.requestAsync("005930")
        assertEquals(1, executed.get())

        currentTime = currentTime.plusMinutes(11)
        service.requestAsync("005930")
        assertEquals(2, executed.get())
    }

    @Test
    fun `backfill-days가 0이면 요청 자체가 no-op이다`() {
        val executed = AtomicInteger()
        val service = service(
            ScriptedFetcher { _, _, _ -> emptyList() },
            InMemoryMinuteStore(),
            backfillDays = 0,
            executor = Executor { executed.incrementAndGet() },
        )

        service.requestAsync("005930")

        assertEquals(0, executed.get())
    }

    private fun before(date: String): String {
        val index = businessDays.indexOf(date)
        return if (index >= 0 && index + 1 < businessDays.size) businessDays[index + 1] else "20260731"
    }
}
