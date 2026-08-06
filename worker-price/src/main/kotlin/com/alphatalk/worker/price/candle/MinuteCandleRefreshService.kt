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
    private val watermarks: MinuteRefreshWatermarkStore,
    private val marketDivs: MinuteMarketDivStore,
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

    fun refresh(code: String): Int =
        coalesced(code, fetchDeadlineMillis) { doRefresh(code, fetchDeadlineMillis, honorFreshness = true) }

    fun syncDay(code: String): Int =
        exclusive(code, dailySyncDeadlineMillis) { doRefresh(code, dailySyncDeadlineMillis, honorFreshness = false) }

    fun isDayComplete(code: String, date: String): Boolean =
        isComplete(code, date, store.latestTime(code, date), marketDivs.get(code))

    private fun isComplete(code: String, date: String, latest: String?, div: String?): Boolean {
        val closeBar = closeTimeOf(div).format(HHMM)
        if ((latest ?: "") >= closeBar) return true
        return (watermarks.fetchedThrough(code, date) ?: "") >= closeBar
    }

    private fun coalesced(code: String, deadlineMillis: Long, work: () -> Int): Int {
        val mine = CompletableFuture<Int>()
        val existing = inFlight.putIfAbsent(code, mine)
        if (existing != null) return awaitExisting(existing)
        return runClaimed(code, deadlineMillis, mine, work)
    }

    private fun exclusive(code: String, deadlineMillis: Long, work: () -> Int): Int {
        repeat(MAX_CLAIM_ATTEMPTS) {
            val mine = CompletableFuture<Int>()
            val existing = inFlight.putIfAbsent(code, mine)
            if (existing == null) return runClaimed(code, deadlineMillis, mine, work)
            runCatching { existing.get(waitTimeoutMillis, TimeUnit.MILLISECONDS) }
        }
        log.warn("minute candle syncDay could not claim in-flight slot: code={}", code)
        return 0
    }

    private fun awaitExisting(existing: CompletableFuture<Int>): Int = try {
        existing.get(waitTimeoutMillis, TimeUnit.MILLISECONDS)
    } catch (e: TimeoutException) {
        0
    } catch (e: ExecutionException) {
        throw e.cause ?: e
    }

    private fun runClaimed(
        code: String,
        deadlineMillis: Long,
        mine: CompletableFuture<Int>,
        work: () -> Int,
    ): Int {
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
        var div = marketDivs.get(code)
        if (isComplete(code, date, store.latestTime(code, date), div)) return 0
        if (honorFreshness) {
            val last = lastFetchedAt[code]
            if (last != null && Duration.between(last, at.toInstant()).seconds < freshSeconds) return 0
        }
        var probing = div == null
        if (probing && store.latestTime(code, date) != null) {
            log.warn(
                "minute candle 시장 구분 캐시가 없는데 당일 저장분이 있다 - 앵커 혼입을 막기 위해 지우고 재적재한다: code={} date={}",
                code,
                date,
            )
            wipeDay(code, date)
        }
        val startedAt = System.nanoTime()
        var total = 0
        repeat(MAX_DIV_PASSES) {
            val outcome = fetchPass(code, date, at, div ?: DIV_UNIFIED, probing, deadlineMillis, startedAt)
            total += outcome.upserted
            if (probing && outcome.resolvedUnified) {
                marketDivs.put(code, DIV_UNIFIED)
                meters.counter("minute.candle.market.div", "div", DIV_UNIFIED).increment()
            }
            if (!outcome.retryAsKrx) {
                lastFetchedAt[code] = at.toInstant()
                return total
            }
            marketDivs.put(code, DIV_KRX)
            meters.counter("minute.candle.market.div", "div", DIV_KRX).increment()
            wipeDay(code, date)
            div = DIV_KRX
            probing = false
        }
        lastFetchedAt[code] = at.toInstant()
        return total
    }

    private fun wipeDay(code: String, date: String) {
        store.deleteDay(code, date)
        watermarks.clear(code, date)
    }

    private data class PassOutcome(
        val upserted: Int,
        val retryAsKrx: Boolean = false,
        val resolvedUnified: Boolean = false,
    )

    private fun fetchPass(
        code: String,
        date: String,
        at: ZonedDateTime,
        div: String,
        probing: Boolean,
        deadlineMillis: Long,
        startedAt: Long,
    ): PassOutcome {
        val openTime = openTimeOf(div)
        val closeTime = closeTimeOf(div)
        val ceiling = minOf(at.toLocalTime().minusMinutes(1), closeTime)
        if (at.toLocalTime() < openTime || ceiling < openTime) return PassOutcome(0)
        val openBar = openTime.format(HHMM)
        val closeBar = closeTime.format(HHMM)
        val latest = store.latestTime(code, date)
        if ((latest ?: "") >= closeBar) return PassOutcome(0)
        val scannedThrough = listOfNotNull(latest, watermarks.fetchedThrough(code, date)).maxOrNull()
        val gapStart = scannedThrough?.let { nextMinute(it) } ?: openBar
        if (gapStart > ceiling.format(HHMM)) return PassOutcome(0)

        val byTime = sortedMapOf<String, KisMinuteCandle>()
        var from = LocalTime.parse(gapStart, HHMM)
        var sawZeroPage = false
        var reachedCeiling = false
        var resolvedUnified = false
        var page = 0
        while (page < MAX_PAGES) {
            if (page > 0 && elapsedMillis(startedAt) >= deadlineMillis) {
                log.warn("minute candle fetch deadline: code={} gapStart={} fetched={}", code, gapStart, byTime.size)
                break
            }
            page += 1
            val to = minOf(from.plusMinutes(PAGE_SPAN_MINUTES), ceiling)
            val chart = fetcher.fetch(code, to, div)
            val zeroPage = chart.candles.isNotEmpty() && chart.candles.all { it.isZeroPriced() }
            if (zeroPage) {
                if (div == DIV_UNIFIED && chart.dailyVolume > 0 && latest == null && byTime.isEmpty()) {
                    log.warn(
                        "minute candle UN 조회가 0봉만 반환했다 - NXT 미지원으로 판정해 KRX로 전환한다: code={} date={} dailyVolume={}",
                        code,
                        date,
                        chart.dailyVolume,
                    )
                    return PassOutcome(0, retryAsKrx = true)
                }
                log.warn(
                    "minute candle 0봉 페이지 - 적재·완주 기록 없이 중단한다: code={} date={} div={} to={} dailyVolume={}",
                    code,
                    date,
                    div,
                    to.format(HHMM),
                    chart.dailyVolume,
                )
                meters.counter("minute.candle.zero.page").increment()
                sawZeroPage = true
                break
            }
            val valid = chart.candles.filter {
                it.date == date && it.time >= openBar && it.time <= closeBar && !it.isZeroPriced()
            }
            if (probing && div == DIV_UNIFIED && valid.isNotEmpty()) resolvedUnified = true
            valid.forEach { byTime[it.time] = it }
            if (to >= ceiling) {
                reachedCeiling = true
                break
            }
            from = to.plusMinutes(1)
        }
        val rows = byTime.values.toList()
        if (rows.isEmpty()) {
            if (reachedCeiling && !sawZeroPage) {
                log.warn(
                    "minute candle refresh: 조회 구간에 봉이 없어 빈 채로 완주 기록한다 - code={} date={} gapStart={} ceiling={}",
                    code,
                    date,
                    gapStart,
                    ceiling.format(HHMM),
                )
                meters.counter("minute.candle.empty.complete").increment()
                watermarks.record(code, date, ceiling.format(HHMM))
            }
            return PassOutcome(0, resolvedUnified = resolvedUnified)
        }
        val upserted = upsertWithRetry(code, date, rows)
        if (reachedCeiling && !sawZeroPage) watermarks.record(code, date, ceiling.format(HHMM))
        meters.counter("minute.candle.refresh").increment(upserted.toDouble())
        log.info("minute candle refresh: code={} date={} div={} gapStart={} rows={}", code, date, div, gapStart, upserted)
        return PassOutcome(upserted, resolvedUnified = resolvedUnified)
    }

    private fun upsertWithRetry(code: String, date: String, rows: List<KisMinuteCandle>): Int = try {
        store.upsert(withMinuteValues(code, date, rows))
    } catch (e: Exception) {
        log.warn("minute candle upsert conflict - 1회 재시도한다: code={} date={}", code, date, e)
        meters.counter("minute.candle.upsert.retry").increment()
        store.upsert(withMinuteValues(code, date, rows))
    }

    private fun withMinuteValues(code: String, date: String, fetched: List<KisMinuteCandle>): List<MinuteCandle> {
        val asc = fetched.sortedBy { it.time }
        var prevAcc = store.sumValueBefore(code, date, asc.first().time)
        return asc.map { candle ->
            if (candle.accValue < prevAcc) {
                log.warn(
                    "minute candle 누적 거래대금이 역행했다 - 시장 구분 전환·재적재 의심: code={} date={} time={} acc={} prevAcc={}",
                    code,
                    date,
                    candle.time,
                    candle.accValue,
                    prevAcc,
                )
                meters.counter("minute.candle.value.regressed").increment()
            }
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

    private fun KisMinuteCandle.isZeroPriced(): Boolean = open == 0L && high == 0L && low == 0L && close == 0L

    private fun openTimeOf(div: String): LocalTime = if (div == DIV_KRX) KRX_OPEN_TIME else OPEN_TIME

    private fun closeTimeOf(div: String?): LocalTime = if (div == DIV_KRX) KRX_CLOSE_TIME else CLOSE_TIME

    private fun elapsedMillis(startedAtNanos: Long): Long = (System.nanoTime() - startedAtNanos) / 1_000_000

    private fun nextMinute(time: String): String = LocalTime.parse(time, HHMM).plusMinutes(1).format(HHMM)

    companion object {
        const val DIV_UNIFIED = "UN"
        const val DIV_KRX = "J"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val HHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmm")
        private val OPEN_TIME: LocalTime = LocalTime.of(8, 0)
        private val CLOSE_TIME: LocalTime = LocalTime.of(20, 0)
        private val KRX_OPEN_TIME: LocalTime = LocalTime.of(9, 0)
        private val KRX_CLOSE_TIME: LocalTime = LocalTime.of(15, 30)
        private const val MAX_PAGES = 26
        private const val MAX_DIV_PASSES = 2
        private const val PAGE_SPAN_MINUTES = 29L
        private const val LOCK_MARGIN_MILLIS = 80_000L
        private const val MAX_CLAIM_ATTEMPTS = 3
    }
}
