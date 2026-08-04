package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteCandle
import com.alphatalk.worker.price.calendar.MarketCalendar
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

class MinuteCandleRefreshService(
    private val fetcher: MinuteCandleFetcher,
    private val store: MinuteCandleStore,
    private val calendar: MarketCalendar,
    private val refreshLock: MinuteRefreshLock,
    private val freshSeconds: Long,
    private val meters: MeterRegistry,
    private val waitTimeoutMillis: Long = 2_000,
    private val fetchDeadlineMillis: Long = 10_000,
    private val dailySyncDeadlineMillis: Long = 120_000,
    private val now: () -> ZonedDateTime = { ZonedDateTime.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val inFlight = ConcurrentHashMap<String, CompletableFuture<Int>>()
    private val lastFetchedAt = ConcurrentHashMap<String, Instant>()
    private val fetchedThrough = ConcurrentHashMap<String, Pair<String, String>>()

    fun refresh(code: String): Int =
        coalesced(code, fetchDeadlineMillis) { doRefresh(code, fetchDeadlineMillis, honorFreshness = true) }

    fun syncDay(code: String): Int =
        coalesced(code, dailySyncDeadlineMillis) { doRefresh(code, dailySyncDeadlineMillis, honorFreshness = false) }

    fun isDayComplete(code: String, date: String): Boolean {
        if ((store.latestTime(code, date) ?: "") >= CLOSE_BAR) return true
        val through = fetchedThrough[code] ?: return false
        return through.first == date && through.second >= CLOSE_BAR
    }

    private fun coalesced(code: String, deadlineMillis: Long, work: () -> Int): Int {
        val mine = CompletableFuture<Int>()
        val existing = inFlight.putIfAbsent(code, mine)
        if (existing != null) {
            return try {
                existing.get(waitTimeoutMillis, TimeUnit.MILLISECONDS)
            } catch (e: TimeoutException) {
                0
            } catch (e: ExecutionException) {
                throw e.cause ?: e
            }
        }
        try {
            val synced = withDistributedLock(code, deadlineMillis, work)
            mine.complete(synced)
            return synced
        } catch (t: Throwable) {
            mine.completeExceptionally(t)
            throw t
        } finally {
            inFlight.remove(code, mine)
        }
    }

    private fun withDistributedLock(code: String, deadlineMillis: Long, work: () -> Int): Int {
        if (!refreshLock.tryAcquire(code, Duration.ofMillis(deadlineMillis + LOCK_MARGIN_MILLIS))) return 0
        return try {
            work()
        } finally {
            refreshLock.release(code)
        }
    }

    private fun doRefresh(code: String, deadlineMillis: Long, honorFreshness: Boolean): Int {
        if (!calendar.isTradingDay()) return 0
        val at = now()
        val date = at.toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE)
        if (isDayComplete(code, date)) return 0
        if (honorFreshness) {
            val last = lastFetchedAt[code]
            if (last != null && Duration.between(last, at.toInstant()).seconds < freshSeconds) return 0
        }
        val ceiling = minOf(at.toLocalTime().minusMinutes(1), CLOSE_TIME)
        if (at.toLocalTime() < OPEN_TIME || ceiling < OPEN_TIME) return 0
        val latest = store.latestTime(code, date)
        val gapStart = latest?.let { nextMinute(it) } ?: OPEN_BAR
        if (gapStart > ceiling.format(HHMM)) {
            lastFetchedAt[code] = at.toInstant()
            return 0
        }
        val outcome = fetchGapForward(code, date, gapStart, ceiling, deadlineMillis)
        if (outcome.rows.isEmpty()) {
            lastFetchedAt[code] = at.toInstant()
            if (outcome.reachedCeiling) fetchedThrough[code] = date to ceiling.format(HHMM)
            return 0
        }
        val upserted = store.upsert(withMinuteValues(code, date, outcome.rows))
        lastFetchedAt[code] = at.toInstant()
        if (outcome.reachedCeiling) fetchedThrough[code] = date to ceiling.format(HHMM)
        meters.counter("minute.candle.refresh").increment(upserted.toDouble())
        log.info("minute candle refresh: code={} date={} gapStart={} rows={}", code, date, gapStart, upserted)
        return upserted
    }

    private data class FetchOutcome(val rows: List<KisMinuteCandle>, val reachedCeiling: Boolean)

    private fun fetchGapForward(
        code: String,
        date: String,
        gapStart: String,
        ceiling: LocalTime,
        deadlineMillis: Long,
    ): FetchOutcome {
        val byTime = sortedMapOf<String, KisMinuteCandle>()
        val startedAt = System.nanoTime()
        var from = LocalTime.parse(gapStart, HHMM)
        repeat(MAX_PAGES) { page ->
            if (page > 0 && elapsedMillis(startedAt) >= deadlineMillis) {
                log.warn("minute candle fetch deadline: code={} gapStart={} fetched={}", code, gapStart, byTime.size)
                return FetchOutcome(byTime.values.toList(), reachedCeiling = false)
            }
            val to = minOf(from.plusMinutes(PAGE_SPAN_MINUTES), ceiling)
            fetcher.fetch(code, to).filter { it.date == date }.forEach { byTime[it.time] = it }
            if (to >= ceiling) return FetchOutcome(byTime.values.toList(), reachedCeiling = true)
            from = to.plusMinutes(1)
        }
        return FetchOutcome(byTime.values.toList(), reachedCeiling = false)
    }

    private fun elapsedMillis(startedAtNanos: Long): Long = (System.nanoTime() - startedAtNanos) / 1_000_000

    private fun withMinuteValues(code: String, date: String, fetched: List<KisMinuteCandle>): List<MinuteCandle> {
        val asc = fetched.sortedBy { it.time }
        var prevAcc = store.sumValueBefore(code, date, asc.first().time)
        return asc.map { candle ->
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

    private fun nextMinute(time: String): String = LocalTime.parse(time, HHMM).plusMinutes(1).format(HHMM)

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val HHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmm")
        private val OPEN_TIME: LocalTime = LocalTime.of(9, 0)
        private val CLOSE_TIME: LocalTime = LocalTime.of(15, 30)
        private const val OPEN_BAR = "0900"
        private const val CLOSE_BAR = "1530"
        private const val MAX_PAGES = 15
        private const val PAGE_SPAN_MINUTES = 29L
        private const val LOCK_MARGIN_MILLIS = 80_000L
    }
}
