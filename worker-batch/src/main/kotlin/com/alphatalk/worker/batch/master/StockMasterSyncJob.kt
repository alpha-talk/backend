package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisMasterParser
import com.alphatalk.kis.master.KisSectorParser
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.kis.master.ParsedSectors
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
    private val sectors: SectorStore,
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
            syncSectors(collected)
            val stored = stocks.upsertAll(collected)
            retireMissing(collected, failedMarkets, incompleteMarkets)
            runs.succeed(runId, stored, failedMarkets.size, clock())
            meters.counter("batch.stock.master.synced").increment(stored.toDouble())
            log.info("stock master sync done: stored={} failedMarkets={}", stored, failedMarkets.size)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun syncSectors(collected: List<KisStockMaster>) {
        val referenced = collected.mapNotNull(KisStockMaster::sectorCode).toSet()
        if (referenced.isEmpty()) {
            log.warn("no sector referenced by collected stocks, skipping sector sync")
            return
        }
        val parsed = fetchSectorsComplete()
        val members = parsed.sectors.filter { it.code in referenced }
        check(members.isNotEmpty()) {
            "sector master has no entry for referenced codes: ${referenced.sorted().take(SAMPLE_SIZE)}"
        }
        val stored = sectors.upsertAll(members)
        meters.counter("batch.sector.synced").increment(stored.toDouble())
        log.info("sector master synced: stored={} referenced={} available={}", stored, referenced.size, parsed.sectors.size)
    }

    private fun fetchSectorsComplete(): ParsedSectors {
        val first = parseSectors()
        if (first.isComplete) return first
        log.warn("sector master had unreadable rows, retrying once: skipped={}", first.skippedLines)
        val second = parseSectors()
        check(second.isComplete) {
            "sector master still incomplete after retry: skipped=${second.skippedLines}"
        }
        return second
    }

    private fun parseSectors(): ParsedSectors {
        val parsed = KisSectorParser.parse(files.fetchSectors())
        check(parsed.sectors.isNotEmpty()) { "sector master file has no entry" }
        return parsed
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
        private const val SAMPLE_SIZE = 5
        private val SEOUL = ZoneId.of("Asia/Seoul")
    }
}
