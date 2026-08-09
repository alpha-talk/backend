package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteCandle
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.market.MarketDivStore
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executor
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class MinuteCandleBackfillService(
    private val fetcher: DailyMinuteCandleFetcher,
    private val store: MinuteCandleStore,
    private val calendar: MarketCalendar,
    private val backfillLock: MinuteBackfillLock,
    private val backfillDays: Int,
    private val meters: MeterRegistry,
    private val cooldownMillis: Long = 600_000,
    private val runBudgetMillis: Long = 120_000,
    executor: Executor? = null,
    private val now: () -> ZonedDateTime = { ZonedDateTime.now(SEOUL) },
    private val nanoTime: () -> Long = System::nanoTime,
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(javaClass)
    private val lastRequestedAt = ConcurrentHashMap<String, Instant>()
    private val ownedExecutor: ExecutorService? = if (executor == null) defaultExecutor() else null
    private val worker: Executor = executor ?: ownedExecutor!!

    fun requestAsync(code: String) {
        if (backfillDays <= 0) return
        val at = now().toInstant()
        var claimed = false
        lastRequestedAt.compute(code) { _, last ->
            if (last != null && Duration.between(last, at).toMillis() < cooldownMillis) {
                last
            } else {
                claimed = true
                at
            }
        }
        if (!claimed) return
        try {
            worker.execute { runBackfill(code) }
        } catch (e: RejectedExecutionException) {
            lastRequestedAt.remove(code, at)
            meters.counter("minute.candle.backfill.rejected").increment()
            log.warn("minute candle backfill 큐가 가득 차 요청을 버린다: code={}", code)
        }
    }

    internal fun runBackfill(code: String) {
        val missing = previousBusinessDays(now().toLocalDate())
            .filter { store.latestTime(code, it.format(DateTimeFormatter.BASIC_ISO_DATE)) == null }
        if (missing.isEmpty()) return
        if (!backfillLock.tryAcquire(code, Duration.ofMillis(runBudgetMillis + LOCK_MARGIN_MILLIS))) return
        try {
            val budget = RunBudget()
            for (date in missing) {
                if (budget.expired()) {
                    meters.counter("minute.candle.backfill.budget").increment()
                    log.warn("minute candle backfill 실행 예산 초과 - 남은 날짜는 다음 트리거로 넘긴다: code={} date={}", code, date)
                    return
                }
                if (store.latestTime(code, date.format(DateTimeFormatter.BASIC_ISO_DATE)) != null) continue
                if (fillDay(code, date, budget) == DayOutcome.ERROR) return
            }
        } finally {
            backfillLock.release(code)
        }
    }

    private enum class DayOutcome { FILLED, NO_DATA, ABORTED, ERROR }

    private inner class RunBudget {
        private val startedAt = nanoTime()
        private var calls = 0

        fun expired(): Boolean = elapsedMillis(startedAt) >= runBudgetMillis

        fun tryConsume(): Boolean {
            if (calls > 0 && expired()) return false
            calls += 1
            return true
        }
    }

    private fun fillDay(code: String, date: LocalDate, budget: RunBudget): DayOutcome {
        for (div in listOf(MarketDivStore.UNIFIED, MarketDivStore.KRX)) {
            val rows = try {
                fetchDay(code, date, div, budget)
            } catch (e: Exception) {
                meters.counter("minute.candle.backfill.error").increment()
                log.warn("minute candle backfill 조회 실패 - 이번 실행을 중단한다: code={} date={} div={}", code, date, div, e)
                return DayOutcome.ERROR
            }
            if (rows == null) {
                meters.counter("minute.candle.backfill.aborted").increment()
                return DayOutcome.ABORTED
            }
            if (rows.isNotEmpty()) {
                val upserted = upsertWithRetry(code, date, withDayValues(rows))
                meters.counter("minute.candle.backfill.rows").increment(upserted.toDouble())
                meters.counter("minute.candle.backfill.day", "div", div).increment()
                log.info("minute candle backfill: code={} date={} div={} rows={}", code, date, div, upserted)
                return DayOutcome.FILLED
            }
        }
        meters.counter("minute.candle.backfill.nodata").increment()
        return DayOutcome.NO_DATA
    }

    private fun fetchDay(code: String, date: LocalDate, div: String, budget: RunBudget): List<KisMinuteCandle>? {
        val dateStr = date.format(DateTimeFormatter.BASIC_ISO_DATE)
        val openTime = if (div == MarketDivStore.KRX) KRX_OPEN_TIME else OPEN_TIME
        val closeTime = if (div == MarketDivStore.KRX) KRX_CLOSE_TIME else CLOSE_TIME
        val openBar = openTime.format(HHMM)
        val closeBar = closeTime.format(HHMM)
        val byTime = sortedMapOf<String, KisMinuteCandle>()
        var to = closeTime
        var pages = 0
        while (pages < MAX_PAGES_PER_DAY) {
            if (!budget.tryConsume()) {
                meters.counter("minute.candle.backfill.budget").increment()
                log.warn("minute candle backfill 실행 예산 초과 - 그날은 적재하지 않는다: code={} date={} div={} fetched={}", code, dateStr, div, byTime.size)
                return null
            }
            pages += 1
            val rows = fetcher.fetch(code, date, to, div)
            val forDate = rows.filter { it.date == dateStr }
            forDate.filter { it.time in openBar..closeBar && !it.isZeroPriced() }
                .forEach { byTime[it.time] = it }
            if (rows.any { it.date < dateStr }) return byTime.values.toList()
            val oldest = forDate.minOfOrNull { it.time }
            if (oldest == null) {
                if (byTime.isEmpty()) return emptyList()
                log.warn(
                    "minute candle backfill 완주 신호 없이 요청일 행이 끊겼다 - 부분 적재를 막기 위해 그날을 버린다: code={} date={} div={} fetched={}",
                    code,
                    dateStr,
                    div,
                    byTime.size,
                )
                return null
            }
            if (oldest <= openBar) return byTime.values.toList()
            val next = LocalTime.parse(oldest, HHMM).minusMinutes(1)
            if (next < openTime) return byTime.values.toList()
            to = next
        }
        log.warn("minute candle backfill 페이지 상한 도달 - 그날은 적재하지 않는다: code={} date={} div={} fetched={}", code, dateStr, div, byTime.size)
        return null
    }

    private fun upsertWithRetry(code: String, date: LocalDate, rows: List<MinuteCandle>): Int = try {
        store.upsert(rows)
    } catch (e: Exception) {
        log.warn("minute candle backfill upsert conflict - 1회 재시도한다: code={} date={}", code, date, e)
        meters.counter("minute.candle.backfill.upsert.retry").increment()
        store.upsert(rows)
    }

    private fun withDayValues(fetched: List<KisMinuteCandle>): List<MinuteCandle> {
        var prevAcc = 0L
        return fetched.map { candle ->
            val value = (candle.accValue - prevAcc).coerceAtLeast(0)
            prevAcc = candle.accValue
            MinuteCandle(
                code = candle.code,
                date = candle.date,
                time = candle.time,
                open = candle.open,
                high = candle.high,
                low = candle.low,
                close = candle.close,
                volume = candle.volume,
                value = value,
            )
        }
    }

    private fun previousBusinessDays(today: LocalDate): List<LocalDate> {
        val result = ArrayList<LocalDate>(backfillDays)
        var date = today.minusDays(1)
        var scanned = 0
        while (result.size < backfillDays && scanned < backfillDays * 5 + 10) {
            if (calendar.isBusinessDay(date)) result += date
            date = date.minusDays(1)
            scanned += 1
        }
        return result
    }

    private fun KisMinuteCandle.isZeroPriced(): Boolean = open == 0L && high == 0L && low == 0L && close == 0L

    private fun elapsedMillis(startedNanos: Long): Long = (nanoTime() - startedNanos) / 1_000_000

    override fun close() {
        ownedExecutor?.shutdownNow()
    }

    private fun defaultExecutor(): ExecutorService = ThreadPoolExecutor(
        1,
        1,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(QUEUE_CAPACITY),
        { runnable -> Thread(runnable, "minute-backfill").apply { isDaemon = true } },
    )

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val HHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmm")
        private val OPEN_TIME: LocalTime = LocalTime.of(8, 0)
        private val CLOSE_TIME: LocalTime = LocalTime.of(20, 0)
        private val KRX_OPEN_TIME: LocalTime = LocalTime.of(9, 0)
        private val KRX_CLOSE_TIME: LocalTime = LocalTime.of(15, 30)
        private const val MAX_PAGES_PER_DAY = 10
        private const val QUEUE_CAPACITY = 256
        private const val LOCK_MARGIN_MILLIS = 80_000L
    }
}
