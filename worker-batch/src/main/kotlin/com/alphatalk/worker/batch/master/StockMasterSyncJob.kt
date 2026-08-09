package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisMasterParser
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.kis.master.ParsedStockMaster
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
    private val stocks: StockMasterStore,
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

    @Scheduled(cron = "\${alphatalk.batch.stock-master.retry-cron:0 0 9,10,12 * * *}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun scheduledRetry() {
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
        var incompleteMarkets = 0
        try {
            KisMarket.entries.forEach { market ->
                val parsed = withOneRetry(market)
                if (parsed == null) {
                    failedMarkets += market
                } else {
                    collected += parsed.stocks
                    if (!parsed.isComplete) {
                        incompleteMarkets += 1
                        log.warn("master file had unreadable rows: market={} skipped={}", market, parsed.skippedLines)
                    }
                }
            }
            val stored = stocks.upsertAll(collected)
            retireMissing(collected, failedMarkets, incompleteMarkets)
            meters.counter("batch.stock.master.synced").increment(stored.toDouble())
            if (failedMarkets.isNotEmpty() || incompleteMarkets > 0) {
                runs.failCounted(
                    runId,
                    stored,
                    failedMarkets.size + incompleteMarkets,
                    "partial universe: failedMarkets=$failedMarkets incompleteMarkets=$incompleteMarkets",
                    clock(),
                )
                log.error(
                    "stock master sync incomplete - universe is partial until a rerun succeeds. stored={} failedMarkets={} incompleteMarkets={}",
                    stored,
                    failedMarkets,
                    incompleteMarkets,
                )
                return stored
            }
            runs.succeed(runId, stored, 0, clock())
            log.info("stock master sync done: stored={}", stored)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun retireMissing(
        collected: List<KisStockMaster>,
        failedMarkets: List<KisMarket>,
        incompleteMarkets: Int,
    ) {
        if (failedMarkets.isNotEmpty()) {
            log.warn("skipping retirement sweep, markets failed: {}", failedMarkets)
            return
        }
        if (incompleteMarkets > 0) {
            log.warn("skipping retirement sweep, {} market file(s) had unreadable rows", incompleteMarkets)
            return
        }
        val retired = stocks.deactivateMissing(collected.map(KisStockMaster::code))
        if (retired > 0) log.info("stock master retired: count={}", retired)
    }

    private fun withOneRetry(market: KisMarket): ParsedStockMaster? {
        val first = runCatching { collect(market) }
        first.getOrNull()?.let { return it }
        log.warn("stock master fetch failed, retrying once: market={}", market, first.exceptionOrNull())
        return runCatching { collect(market) }
            .onFailure { log.warn("stock master fetch failed after retry: market={}", market, it) }
            .getOrNull()
    }

    private fun collect(market: KisMarket): ParsedStockMaster {
        val parsed = KisMasterParser.parse(market, files.fetch(market))
        val commonStocks = parsed.stocks.filter(KisStockMaster::isCommonStock)
        check(commonStocks.isNotEmpty()) { "master file has no common stock: market=$market" }
        return ParsedStockMaster(commonStocks, parsed.skippedLines)
    }

    companion object {
        const val JOB_NAME = "stock_master_sync"
        private val SEOUL = ZoneId.of("Asia/Seoul")
    }
}
