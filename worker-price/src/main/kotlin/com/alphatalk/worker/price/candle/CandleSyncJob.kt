package com.alphatalk.worker.price.candle

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
    private val meters: MeterRegistry,
    private val today: () -> LocalDate = { LocalDate.now(ZoneId.of("Asia/Seoul")) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 30 16 * * MON-FRI", zone = "Asia/Seoul")
    fun syncDaily() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val to = today()
        var synced = 0
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
                log.warn("candle sync failed: code={}", code, it)
            }
        }
        if (synced > 0) {
            meters.counter("candle.sync").increment(synced.toDouble())
        }
        return synced
    }
}
