package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisMasterParser
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

open class StockMasterSyncJob(
    private val files: MasterFileFetcher,
    private val store: StockMasterStore,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.stock-master.cron:0 0 8 * * *}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val runDate = today().format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock())
        if (runId == null) {
            log.info("stock master sync skipped, already succeeded today: runDate={}", runDate)
            return 0
        }
        val collected = mutableListOf<KisStockMaster>()
        val failedMarkets = mutableListOf<KisMarket>()
        try {
            KisMarket.entries.forEach { market ->
                runCatching { collect(market) }
                    .onSuccess { collected += it }
                    .onFailure {
                        failedMarkets += market
                        log.warn("stock master fetch failed: market={}", market, it)
                    }
            }
            val stored = store.upsertAll(collected)
            if (failedMarkets.isEmpty()) {
                val retired = store.deactivateMissing(collected.map(KisStockMaster::code))
                if (retired > 0) log.info("stock master retired: count={}", retired)
            } else {
                log.warn("skipping retirement sweep, markets failed: {}", failedMarkets)
            }
            runs.succeed(runId, stored, failedMarkets.size, clock())
            meters.counter("batch.stock.master.synced").increment(stored.toDouble())
            log.info("stock master sync done: stored={} failedMarkets={}", stored, failedMarkets.size)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun collect(market: KisMarket): List<KisStockMaster> {
        val stocks = KisMasterParser.parseAll(market, files.fetch(market))
            .filter(KisStockMaster::isCommonStock)
        check(stocks.isNotEmpty()) { "master file has no common stock: market=$market" }
        return stocks
    }

    companion object {
        const val JOB_NAME = "stock_master_sync"
        private val SEOUL = ZoneId.of("Asia/Seoul")
    }
}
