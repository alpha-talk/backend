package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.kis.rest.KisInvestorFlow
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

fun interface InvestorFlowFetcher {
    fun fetch(code: String): List<KisInvestorFlow>
}

open class InvestorFlowSyncJob(
    private val universe: StockUniverse,
    private val fetcher: InvestorFlowFetcher,
    private val store: InvestorFlowStore,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val holidays: Set<LocalDate> = emptySet(),
    private val deadline: Duration = Duration.ofMinutes(25),
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.investor.cron:0 10 17 * * MON-FRI}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT30M", lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val date = today()
        if (!isBusinessDay(date)) return 0
        val runDate = date.format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock())
        if (runId == null) {
            log.info("investor flow sync skipped, already succeeded today: runDate={}", runDate)
            return 0
        }
        try {
            val stocks = universe.activeStocks()
            if (stocks.isEmpty()) {
                log.error("investor flow sync has no universe: stock_master is empty - stock_master_sync must land first")
                runs.failCounted(runId, 0, 0, "stock_master is empty", clock())
                return 0
            }
            val deadlineAt = clock().plus(deadline)
            var stored = 0
            val failed = mutableListOf<ActiveStock>()
            for ((index, stock) in stocks.withIndex()) {
                if (clock() >= deadlineAt) return abortOnDeadline(runId, stored, failed.size + stocks.size - index)
                val rows = fetchRows(stock.code)
                if (rows == null) {
                    failed += stock
                    continue
                }
                stored += store.upsert(rows)
            }
            var failCount = 0
            for ((index, stock) in failed.withIndex()) {
                if (clock() >= deadlineAt) return abortOnDeadline(runId, stored, failCount + failed.size - index)
                val rows = fetchRows(stock.code)
                if (rows == null) {
                    failCount += 1
                    log.warn("investor flow fetch failed after retry, recovers tomorrow: code={}", stock.code)
                } else {
                    stored += store.upsert(rows)
                }
            }
            meters.counter("batch.investor.synced").increment(stored.toDouble())
            if (failCount > 0) meters.counter("batch.investor.failed").increment(failCount.toDouble())
            runs.succeed(runId, stored, failCount, clock())
            log.info("investor flow sync done: rows={} failed={}", stored, failCount)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun fetchRows(code: String): List<InvestorFlowRow>? = try {
        fetcher.fetch(code).map {
            InvestorFlowRow(
                code = it.code,
                date = it.date,
                individual = it.individual,
                foreign = it.foreign,
                institution = it.institution,
            )
        }
    } catch (e: Exception) {
        log.warn("investor flow fetch failed: code={}", code, e)
        null
    }

    private fun abortOnDeadline(runId: Long, stored: Int, unresolved: Int): Int {
        meters.counter("batch.investor.deadline").increment()
        log.error(
            "investor flow sync deadline reached before lock expiry - marked FAILED for manual rerun. stored={} unresolved={}",
            stored,
            unresolved,
        )
        runs.failCounted(runId, stored, unresolved, "deadline reached: unresolved=$unresolved", clock())
        return stored
    }

    private fun isBusinessDay(date: LocalDate): Boolean =
        date.dayOfWeek != DayOfWeek.SATURDAY && date.dayOfWeek != DayOfWeek.SUNDAY && date !in holidays

    companion object {
        const val JOB_NAME = "investor_flow_daily"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
