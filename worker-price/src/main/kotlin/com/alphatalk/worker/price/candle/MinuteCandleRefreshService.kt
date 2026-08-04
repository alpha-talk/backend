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
    private val freshSeconds: Long,
    private val meters: MeterRegistry,
    private val waitTimeoutMillis: Long = 2_000,
    private val now: () -> ZonedDateTime = { ZonedDateTime.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val inFlight = ConcurrentHashMap<String, CompletableFuture<Int>>()
    private val lastFetchedAt = ConcurrentHashMap<String, Instant>()

    fun refresh(code: String): Int {
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
            val synced = doRefresh(code)
            mine.complete(synced)
            return synced
        } catch (t: Throwable) {
            mine.completeExceptionally(t)
            throw t
        } finally {
            inFlight.remove(code, mine)
        }
    }

    private fun doRefresh(code: String): Int {
        if (!calendar.isTradingDay()) return 0
        val at = now()
        if (at.toLocalTime() < OPEN_TIME) return 0
        val date = at.toLocalDate().format(DateTimeFormatter.BASIC_ISO_DATE)
        val latest = store.latestTime(code, date)
        if (latest != null && latest >= CLOSE_BAR) return 0
        val last = lastFetchedAt[code]
        if (last != null && Duration.between(last, at.toInstant()).seconds < freshSeconds) return 0
        val gapStart = latest?.let { nextMinute(it) } ?: OPEN_BAR
        val ceiling = minOf(at.toLocalTime(), CLOSE_TIME)
        val fetched = fetchGap(code, date, gapStart, ceiling)
        lastFetchedAt[code] = at.toInstant()
        if (fetched.isEmpty()) return 0
        val upserted = store.upsert(withMinuteValues(code, date, fetched))
        meters.counter("minute.candle.refresh").increment(upserted.toDouble())
        log.info("minute candle refresh: code={} date={} gapStart={} rows={}", code, date, gapStart, upserted)
        return upserted
    }

    private fun fetchGap(code: String, date: String, gapStart: String, ceiling: LocalTime): List<KisMinuteCandle> {
        val byTime = sortedMapOf<String, KisMinuteCandle>()
        var to = ceiling
        repeat(MAX_PAGES) {
            val page = fetcher.fetch(code, to).filter { it.date == date }
            if (page.isEmpty()) return byTime.values.toList()
            page.forEach { byTime[it.time] = it }
            val earliest = page.minOf { it.time }
            if (earliest <= gapStart) return byTime.values.toList()
            val nextTo = LocalTime.parse(earliest, HHMM).minusMinutes(1)
            if (nextTo < OPEN_TIME) return byTime.values.toList()
            to = nextTo
        }
        return byTime.values.toList()
    }

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
    }
}
