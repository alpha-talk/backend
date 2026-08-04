package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.leader.LeaderLock
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class CandleSyncJob(
    private val symbols: () -> Set<String>,
    private val fetcher: DailyCandleFetcher,
    private val store: DailyCandleStore,
    private val backfillDays: Long,
    private val calendar: MarketCalendar,
    private val leader: LeaderLock,
    private val meters: MeterRegistry,
    private val today: () -> LocalDate = { LocalDate.now(ZoneId.of("Asia/Seoul")) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Volatile
    private var completedDate: LocalDate? = null

    @Scheduled(cron = "0 30 16 * * MON-FRI", zone = "Asia/Seoul")
    fun syncDaily() {
        if (!calendar.isTradingDay()) return
        if (!leader.tryAcquire()) return
        attemptSync()
    }

    @Scheduled(cron = "0 0 17,18,19 * * MON-FRI", zone = "Asia/Seoul")
    fun retryUnfinished() {
        if (!calendar.isTradingDay()) return
        if (completedDate == today()) return
        if (!leader.tryAcquire()) return
        log.warn("candle sync retry: 정기 회차가 완료되지 않았다. date={}", today())
        attemptSync()
    }

    private fun attemptSync() {
        val result = try {
            syncOnce()
        } catch (e: Exception) {
            meters.counter("candle.sync.aborted").increment()
            log.error("candle sync aborted - 다음 회차에 재시도한다", e)
            return
        }
        if (result.complete) {
            completedDate = today()
            return
        }
        log.error(
            "candle sync incomplete - 다음 회차에 재시도한다. synced={} failed={}",
            result.synced,
            result.failed,
        )
    }

    fun syncOnce(): CandleSyncResult {
        val to = today()
        var synced = 0
        var failed = 0
        symbols().forEach { code ->
            runCatching {
                val from = store.latestDate(code)
                    ?.let { LocalDate.parse(it, DateTimeFormatter.BASIC_ISO_DATE).plusDays(1) }
                    ?: to.minusDays(backfillDays)
                if (from <= to) {
                    val upserted = store.upsert(fetcher.fetch(code, from, to))
                    log.info("candle sync: code={} from={} to={} rows={}", code, from, to, upserted)
                    synced += 1
                }
            }.onFailure {
                failed += 1
                log.warn("candle sync failed: code={}", code, it)
            }
        }
        if (synced > 0) {
            meters.counter("candle.sync").increment(synced.toDouble())
        }
        if (failed > 0) {
            meters.counter("candle.sync.failed").increment(failed.toDouble())
        }
        return CandleSyncResult(synced = synced, failed = failed)
    }
}

data class CandleSyncResult(val synced: Int, val failed: Int) {
    val complete: Boolean get() = failed == 0
}
