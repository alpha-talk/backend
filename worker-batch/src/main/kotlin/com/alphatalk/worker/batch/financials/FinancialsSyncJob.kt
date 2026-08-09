package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartApiException
import com.alphatalk.worker.batch.industry.DartClient
import com.alphatalk.worker.batch.industry.DartFinancialAccount
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.master.StockMasterSyncJob
import com.alphatalk.worker.batch.stockinfo.StockUniverse
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

data class FinancialTarget(
    val code: String,
    val corpCode: String,
    val year: Int,
    val reprtCode: String,
    val receiptDate: String,
)

open class FinancialsSyncJob(
    private val dart: DartClient,
    private val universe: StockUniverse,
    private val store: FinancialSummaryStore,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val lookbackDays: Long = 7,
    private val prerequisiteJob: String? = StockMasterSyncJob.JOB_NAME,
    private val requestInterval: Duration = Duration.ofMillis(50),
    private val deadline: Duration = Duration.ofMinutes(100),
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
    private val pause: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.financials.cron:0 0 6 * * *}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT2H", lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    @Scheduled(cron = "\${alphatalk.batch.financials.retry-cron:0 0 9,13,17 * * *}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT2H", lockAtLeastFor = "PT1M")
    open fun scheduledRetry() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val date = today()
        val runDate = date.format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock())
        if (runId == null) {
            log.info("financials sync skipped, already succeeded today: runDate={}", runDate)
            return 0
        }
        try {
            if (prerequisiteJob != null && runs.hasFailedRun(prerequisiteJob, runDate)) {
                log.error("financials sync defers: {} ran today but is not SUCCESS - universe may be partial", prerequisiteJob)
                runs.failCounted(runId, 0, 0, "$prerequisiteJob incomplete today", clock())
                return 0
            }
            val deadlineAt = clock().plus(deadline)
            val targets = detectTargets(date)
            var stored = 0
            val failed = mutableListOf<FinancialTarget>()
            for ((index, target) in targets.withIndex()) {
                if (clock() >= deadlineAt) return abortOnDeadline(runId, stored, failed.size + targets.size - index)
                when (process(target)) {
                    ProcessResult.STORED -> stored += 1
                    ProcessResult.ABSENT -> Unit
                    ProcessResult.FAILED -> failed += target
                }
            }
            var failCount = 0
            for ((index, target) in failed.withIndex()) {
                if (clock() >= deadlineAt) return abortOnDeadline(runId, stored, failCount + failed.size - index)
                when (process(target)) {
                    ProcessResult.STORED -> stored += 1
                    ProcessResult.ABSENT -> Unit
                    ProcessResult.FAILED -> {
                        failCount += 1
                        log.warn(
                            "financials fetch failed after retry, next run retries within lookback: code={} year={} reprt={}",
                            target.code,
                            target.year,
                            target.reprtCode,
                        )
                    }
                }
            }
            meters.counter("batch.financials.synced").increment(stored.toDouble())
            if (failCount > 0) {
                meters.counter("batch.financials.failed").increment(failCount.toDouble())
                runs.failCounted(runId, stored, failCount, "unresolved targets=$failCount", clock())
                log.error(
                    "financials sync left targets unresolved - they leave the {}d detection window on later runs. stored={} failed={}",
                    lookbackDays,
                    stored,
                    failCount,
                )
                return stored
            }
            runs.succeed(runId, stored, 0, clock())
            log.info("financials sync done: targets={} stored={}", targets.size, stored)
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun detectTargets(date: LocalDate): List<FinancialTarget> {
        val active = universe.activeCodes()
        check(active.isNotEmpty()) { "stock_master is empty - stock_master_sync must land first" }
        pause(requestInterval)
        val disclosures = dart.periodicDisclosures(date.minusDays(lookbackDays), date)
        if (disclosures.isEmpty()) return emptyList()
        return disclosures.mapNotNull { disclosure ->
            val code = disclosure.stockCode ?: return@mapNotNull null
            if (code !in active) return@mapNotNull null
            val report = ReportReference.parse(disclosure.reportName) ?: return@mapNotNull null
            FinancialTarget(
                code = code,
                corpCode = disclosure.corpCode,
                year = report.year,
                reprtCode = report.reprtCode,
                receiptDate = disclosure.receiptDate,
            )
        }
            .groupBy { Triple(it.code, it.year, it.reprtCode) }
            .map { (_, group) -> group.maxBy(FinancialTarget::receiptDate) }
    }

    private fun process(target: FinancialTarget): ProcessResult {
        val fetched = try {
            fetchAccounts(target)
        } catch (e: DartApiException) {
            if (e.status in FATAL_STATUSES) throw e
            log.warn("financials fetch failed: code={} year={} reprt={}", target.code, target.year, target.reprtCode, e)
            return ProcessResult.FAILED
        } catch (e: Exception) {
            log.warn("financials fetch failed: code={} year={} reprt={}", target.code, target.year, target.reprtCode, e)
            return ProcessResult.FAILED
        }
        if (fetched == null) {
            meters.counter("batch.financials.absent").increment()
            return ProcessResult.ABSENT
        }
        val figures = FinancialAccountMapper.map(fetched.accounts)
        if (figures.isEmpty) {
            meters.counter("batch.financials.unmapped").increment()
            log.warn(
                "financial accounts mapped nothing, skipping: code={} year={} reprt={} fsDiv={}",
                target.code,
                target.year,
                fetched.reprtCode,
                fetched.fsDiv,
            )
            return ProcessResult.ABSENT
        }
        store.upsert(
            FinancialSummaryRow(
                code = target.code,
                year = target.year,
                reprtCode = fetched.reprtCode,
                fsDiv = fetched.fsDiv,
                figures = figures,
                disclosedAt = disclosedAt(target.receiptDate),
            ),
        )
        return ProcessResult.STORED
    }

    private fun fetchAccounts(target: FinancialTarget): FetchedAccounts? {
        for (fsDiv in listOf(CONSOLIDATED, SEPARATE)) {
            pause(requestInterval)
            val accounts = dart.financialAccounts(target.corpCode, target.year, target.reprtCode, fsDiv)
            if (accounts.isNotEmpty()) return FetchedAccounts(target.reprtCode, fsDiv, accounts)
        }
        return null
    }

    private fun abortOnDeadline(runId: Long, stored: Int, unresolved: Int): Int {
        meters.counter("batch.financials.deadline").increment()
        log.error(
            "financials sync deadline reached before lock expiry - marked FAILED for manual rerun. stored={} unresolved={}",
            stored,
            unresolved,
        )
        runs.failCounted(runId, stored, unresolved, "deadline reached: unresolved=$unresolved", clock())
        return stored
    }

    private fun disclosedAt(receiptDate: String): Instant =
        LocalDate.parse(receiptDate, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(SEOUL).toInstant()

    private data class FetchedAccounts(
        val reprtCode: String,
        val fsDiv: String,
        val accounts: List<DartFinancialAccount>,
    )

    private enum class ProcessResult { STORED, ABSENT, FAILED }

    companion object {
        const val JOB_NAME = "financials_sync"
        const val CONSOLIDATED = "CFS"
        const val SEPARATE = "OFS"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val FATAL_STATUSES = setOf("010", "011", "012", "020", "021", "100", "101", "800", "901")
    }
}
