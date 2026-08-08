package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.kis.rest.KisValuationSnapshot
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.DayOfWeek
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

fun interface ValuationFetcher {
    fun fetch(code: String): KisValuationSnapshot
}

open class ValuationSyncJob(
    private val universe: StockUniverse,
    private val fetcher: ValuationFetcher,
    private val store: ValuationStore,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val holidays: Set<LocalDate> = emptySet(),
    private val chunkSize: Int = 200,
    private val deadline: Duration = Duration.ofMinutes(25),
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.valuation.cron:0 50 16 * * MON-FRI}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    @Scheduled(cron = "\${alphatalk.batch.valuation.retry-cron:0 20 17,18 * * MON-FRI}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun scheduledRetry() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val date = today()
        if (!isBusinessDay(date)) return 0
        val runDate = date.format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock())
        if (runId == null) {
            log.info("valuation sync skipped, already succeeded today: runDate={}", runDate)
            return 0
        }
        try {
            val stocks = universe.activeStocks()
            if (stocks.isEmpty()) {
                log.error("valuation sync has no universe: stock_master is empty - stock_master_sync must land first")
                runs.failCounted(runId, 0, 0, "stock_master is empty", clock())
                return 0
            }
            val deadlineAt = clock().plus(deadline)
            var stored = 0
            val buffer = mutableListOf<ValuationRow>()
            val failed = mutableListOf<ActiveStock>()
            for ((index, stock) in stocks.withIndex()) {
                if (clock() >= deadlineAt) {
                    stored += store.upsert(buffer)
                    return abortOnDeadline(runId, stored, failed.size + stocks.size - index)
                }
                val row = fetchRow(stock, runDate)
                if (row == null) {
                    failed += stock
                    continue
                }
                buffer += row
                if (buffer.size >= chunkSize) {
                    stored += store.upsert(buffer)
                    buffer.clear()
                }
            }
            var failCount = 0
            for ((index, stock) in failed.withIndex()) {
                if (clock() >= deadlineAt) {
                    stored += store.upsert(buffer)
                    return abortOnDeadline(runId, stored, failCount + failed.size - index)
                }
                val row = fetchRow(stock, runDate)
                if (row == null) {
                    failCount += 1
                    log.warn("valuation fetch failed after retry, recovers tomorrow: code={}", stock.code)
                    continue
                }
                buffer += row
                if (buffer.size >= chunkSize) {
                    stored += store.upsert(buffer)
                    buffer.clear()
                }
            }
            stored += store.upsert(buffer)
            meters.counter("batch.valuation.synced").increment(stored.toDouble())
            if (failCount > 0) {
                meters.counter("batch.valuation.failed").increment(failCount.toDouble())
                runs.failCounted(runId, stored, failCount, "partial failure: failed=$failCount", clock())
                log.error(
                    "valuation sync incomplete - retry cron or manual rerun completes today's rows. stored={} failed={}",
                    stored,
                    failCount,
                )
                return stored
            }
            runs.succeed(runId, stored, 0, clock())
            log.info("valuation sync done: stored={}", stored)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun fetchRow(stock: ActiveStock, runDate: String): ValuationRow? = try {
        val snapshot = fetcher.fetch(stock.code)
        ValuationRow(
            code = stock.code,
            date = runDate,
            per = snapshot.per,
            pbr = snapshot.pbr,
            eps = snapshot.eps,
            bps = snapshot.bps,
            marketCap = stock.sharesOutstanding?.let { Math.multiplyExact(it, snapshot.price) },
        )
    } catch (e: Exception) {
        log.warn("valuation fetch failed: code={}", stock.code, e)
        null
    }

    private fun abortOnDeadline(runId: Long, stored: Int, unresolved: Int): Int {
        meters.counter("batch.valuation.deadline").increment()
        log.error(
            "valuation sync deadline reached before lock expiry - marked FAILED for manual rerun. stored={} unresolved={}",
            stored,
            unresolved,
        )
        runs.failCounted(runId, stored, unresolved, "deadline reached: unresolved=$unresolved", clock())
        return stored
    }

    private fun isBusinessDay(date: LocalDate): Boolean =
        date.dayOfWeek != DayOfWeek.SATURDAY && date.dayOfWeek != DayOfWeek.SUNDAY && date !in holidays

    companion object {
        const val JOB_NAME = "valuation_daily"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
