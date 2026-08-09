package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartApiException
import com.alphatalk.worker.batch.industry.DartClient
import com.alphatalk.worker.batch.industry.DartDisclosure
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
    private val corps: CorpDirectory,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val lookbackDays: Long = 7,
    private val backfillYears: Long = 3,
    private val backfillPerRun: Int = 100,
    private val failureStreakLimit: Int = 5,
    private val prerequisiteJob: String? = StockMasterSyncJob.JOB_NAME,
    private val requestInterval: Duration = Duration.ofMillis(50),
    private val deadline: Duration = Duration.ofMinutes(100),
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
    private val pause: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        require(failureStreakLimit >= 1) {
            "alphatalk.batch.financials.failure-streak-limit는 1 이상이어야 한다: $failureStreakLimit"
        }
        require(backfillYears > HISTORY_COVERAGE_YEARS) {
            "alphatalk.batch.financials.backfill-years는 커버리지 판정 연차($HISTORY_COVERAGE_YEARS)보다 커야 한다 — " +
                "작으면 백필 창이 판정 연도의 보고서를 못 잡아 같은 종목을 매 회차 재시도한다: $backfillYears"
        }
    }

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
            if (prerequisiteJob != null && runs.hasIncompleteRun(prerequisiteJob, runDate)) {
                log.error("financials sync defers: {} ran today but is not SUCCESS - universe may be partial", prerequisiteJob)
                runs.failCounted(runId, 0, 0, "$prerequisiteJob incomplete today", clock())
                return 0
            }
            val active = universe.activeCodes()
            check(active.isNotEmpty()) { "stock_master is empty - stock_master_sync must land first" }
            val deadlineAt = clock().plus(deadline)
            val tally = Tally()
            var abort = processWithRetry(tally, detectTargets(date, active), deadlineAt)
            if (abort == null) abort = backfill(tally, date, active, deadlineAt)
            meters.counter("batch.financials.synced").increment(tally.stored.toDouble())
            if (abort != null) {
                meters.counter(abort.metric).increment()
                runs.failCounted(runId, tally.stored, abort.unresolved, abort.reason, clock())
                log.error(
                    "financials sync aborted - marked FAILED for rerun: {} stored={} unresolved={}",
                    abort.reason,
                    tally.stored,
                    abort.unresolved,
                )
                return tally.stored
            }
            if (tally.failCount > 0) {
                meters.counter("batch.financials.failed").increment(tally.failCount.toDouble())
                runs.failCounted(runId, tally.stored, tally.failCount, "unresolved targets=${tally.failCount}", clock())
                log.error(
                    "financials sync left targets unresolved - they leave the {}d detection window on later runs. stored={} failed={}",
                    lookbackDays,
                    tally.stored,
                    tally.failCount,
                )
                return tally.stored
            }
            runs.succeed(runId, tally.stored, 0, clock())
            log.info("financials sync done: stored={}", tally.stored)
            return tally.stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun detectTargets(date: LocalDate, active: Set<String>): List<FinancialTarget> {
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
        }.let(::dedupLatest)
    }

    private fun backfill(tally: Tally, date: LocalDate, active: Set<String>, deadlineAt: Instant): Abort? {
        val candidates = backfillCandidates(active, date)
        if (candidates.isEmpty()) return null
        meters.counter("batch.financials.backfill.stocks").increment(candidates.size.toDouble())
        log.info("financials backfill: stocks={} window={}y per-run-cap={}", candidates.size, backfillYears, backfillPerRun)
        for ((index, candidate) in candidates.withIndex()) {
            val (code, corpCode) = candidate
            checkAbort(tally, deadlineAt, tally.failCount + candidates.size - index)?.let { return it }
            val outcome = try {
                backfillStock(code, corpCode, date, deadlineAt)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            } catch (e: DartApiException) {
                if (isFatal(e.status)) throw e
                BackfillOutcome.StockFailed.also {
                    log.warn("financials backfill failed, next run retries while coverage stays missing: code={}", code, e)
                }
            }
            when (outcome) {
                is BackfillOutcome.Done -> {
                    store.upsertAll(outcome.rows)
                    tally.stored += outcome.rows.size
                    tally.streak = 0
                }
                BackfillOutcome.StockFailed -> {
                    tally.streak += 1
                    tally.failCount += 1
                }
                BackfillOutcome.DeadlineReached -> {
                    val unresolved = tally.failCount + candidates.size - index
                    return Abort(
                        reason = "deadline reached: unresolved=$unresolved",
                        unresolved = unresolved,
                        metric = "batch.financials.deadline",
                    )
                }
            }
        }
        return null
    }

    private fun backfillStock(code: String, corpCode: String, date: LocalDate, deadlineAt: Instant): BackfillOutcome {
        val targets = try {
            backfillTargetsFor(code, corpCode, date, deadlineAt)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: DartApiException) {
            throw e
        } catch (e: Exception) {
            log.warn("financials backfill list failed: code={}", code, e)
            return BackfillOutcome.StockFailed
        } ?: return BackfillOutcome.DeadlineReached
        val rows = mutableListOf<FinancialSummaryRow>()
        val deferred = mutableListOf<FinancialTarget>()
        for (target in targets) {
            if (clock() >= deadlineAt) return BackfillOutcome.DeadlineReached
            when (val outcome = evaluate(target)) {
                is Evaluation.Resolved -> rows += outcome.row
                Evaluation.Absent -> Unit
                Evaluation.Failed -> deferred += target
            }
        }
        for (target in deferred) {
            if (clock() >= deadlineAt) return BackfillOutcome.DeadlineReached
            when (val outcome = evaluate(target)) {
                is Evaluation.Resolved -> rows += outcome.row
                Evaluation.Absent -> Unit
                Evaluation.Failed -> {
                    log.warn(
                        "financials backfill defers whole stock - partial rows would hide the gap forever: code={} year={} reprt={}",
                        target.code,
                        target.year,
                        target.reprtCode,
                    )
                    return BackfillOutcome.StockFailed
                }
            }
        }
        return BackfillOutcome.Done(rows)
    }

    private fun backfillTargetsFor(code: String, corpCode: String, date: LocalDate, deadlineAt: Instant): List<FinancialTarget>? {
        val disclosures = mutableListOf<DartDisclosure>()
        var windowStart = date.minusYears(backfillYears)
        while (windowStart <= date) {
            if (clock() >= deadlineAt) return null
            val windowEnd = minOf(windowStart.plusYears(1).minusDays(1), date)
            pause(requestInterval)
            disclosures += dart.periodicDisclosures(windowStart, windowEnd, corpCode)
            windowStart = windowEnd.plusDays(1)
        }
        return disclosures.mapNotNull { disclosure ->
            val report = ReportReference.parse(disclosure.reportName) ?: return@mapNotNull null
            FinancialTarget(
                code = code,
                corpCode = corpCode,
                year = report.year,
                reprtCode = report.reprtCode,
                receiptDate = disclosure.receiptDate,
            )
        }.let(::dedupLatest)
    }

    private fun backfillCandidates(active: Set<String>, date: LocalDate): List<Pair<String, String>> {
        if (backfillPerRun <= 0) return emptyList()
        val missing = (active - store.codesWithRowOnOrBefore(date.year - HISTORY_COVERAGE_YEARS)).sorted()
        if (missing.isEmpty()) return emptyList()
        val corpByCode = corps.corpCodesFor(missing)
        val mapped = missing.mapNotNull { code -> corpByCode[code]?.let { code to it } }
        val unmapped = missing.size - mapped.size
        if (unmapped > 0) {
            meters.counter("batch.financials.backfill.unmapped").increment(unmapped.toDouble())
            log.warn("financials backfill skips {} codes without dart_corp_map entry - dart_corp_map(주 1회)이 채우면 합류한다", unmapped)
        }
        if (mapped.size <= backfillPerRun) return mapped
        val start = ((date.toEpochDay() * backfillPerRun) % mapped.size).toInt()
        return List(backfillPerRun) { offset -> mapped[(start + offset) % mapped.size] }
    }

    private fun processWithRetry(tally: Tally, targets: List<FinancialTarget>, deadlineAt: Instant): Abort? {
        val deferred = mutableListOf<FinancialTarget>()
        for ((index, target) in targets.withIndex()) {
            checkAbort(tally, deadlineAt, tally.failCount + deferred.size + targets.size - index)?.let { return it }
            when (process(target)) {
                ProcessResult.STORED -> {
                    tally.stored += 1
                    tally.streak = 0
                }
                ProcessResult.ABSENT -> tally.streak = 0
                ProcessResult.FAILED -> {
                    tally.streak += 1
                    deferred += target
                }
            }
        }
        for ((index, target) in deferred.withIndex()) {
            checkAbort(tally, deadlineAt, tally.failCount + deferred.size - index)?.let { return it }
            when (process(target)) {
                ProcessResult.STORED -> {
                    tally.stored += 1
                    tally.streak = 0
                }
                ProcessResult.ABSENT -> tally.streak = 0
                ProcessResult.FAILED -> {
                    tally.streak += 1
                    tally.failCount += 1
                    log.warn(
                        "financials target unresolved after retry: code={} year={} reprt={}",
                        target.code,
                        target.year,
                        target.reprtCode,
                    )
                }
            }
        }
        return null
    }

    private fun checkAbort(tally: Tally, deadlineAt: Instant, unresolved: Int): Abort? = when {
        clock() >= deadlineAt -> Abort(
            reason = "deadline reached: unresolved=$unresolved",
            unresolved = unresolved,
            metric = "batch.financials.deadline",
        )
        tally.streak >= failureStreakLimit -> Abort(
            reason = "failure streak=${tally.streak} - upstream outage suspected: unresolved=$unresolved",
            unresolved = unresolved,
            metric = "batch.financials.breaker",
        )
        else -> null
    }

    private fun process(target: FinancialTarget): ProcessResult = when (val outcome = evaluate(target)) {
        is Evaluation.Resolved -> {
            store.upsert(outcome.row)
            ProcessResult.STORED
        }
        Evaluation.Absent -> ProcessResult.ABSENT
        Evaluation.Failed -> ProcessResult.FAILED
    }

    private fun evaluate(target: FinancialTarget): Evaluation {
        val fetched = try {
            fetchAccounts(target)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: DartApiException) {
            if (isFatal(e.status)) throw e
            log.warn("financials fetch failed: code={} year={} reprt={}", target.code, target.year, target.reprtCode, e)
            return Evaluation.Failed
        } catch (e: Exception) {
            log.warn("financials fetch failed: code={} year={} reprt={}", target.code, target.year, target.reprtCode, e)
            return Evaluation.Failed
        }
        if (fetched == null) {
            meters.counter("batch.financials.absent").increment()
            return Evaluation.Absent
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
            return Evaluation.Absent
        }
        return Evaluation.Resolved(
            FinancialSummaryRow(
                code = target.code,
                year = target.year,
                reprtCode = fetched.reprtCode,
                fsDiv = fetched.fsDiv,
                figures = figures,
                disclosedAt = disclosedAt(target.receiptDate),
            ),
        )
    }

    private fun fetchAccounts(target: FinancialTarget): FetchedAccounts? {
        for (fsDiv in listOf(CONSOLIDATED, SEPARATE)) {
            pause(requestInterval)
            val accounts = dart.financialAccounts(target.corpCode, target.year, target.reprtCode, fsDiv)
            if (accounts.isNotEmpty()) return FetchedAccounts(target.reprtCode, fsDiv, accounts)
        }
        return null
    }

    private fun dedupLatest(targets: List<FinancialTarget>): List<FinancialTarget> =
        targets.groupBy { Triple(it.code, it.year, it.reprtCode) }
            .map { (_, group) -> group.maxBy(FinancialTarget::receiptDate) }

    private fun isFatal(status: String): Boolean =
        status in FATAL_STATUSES || status.toIntOrNull()?.let { it == 429 || it in 500..599 } == true

    private fun disclosedAt(receiptDate: String): Instant =
        LocalDate.parse(receiptDate, DateTimeFormatter.BASIC_ISO_DATE).atStartOfDay(SEOUL).toInstant()

    private class Tally {
        var stored = 0
        var failCount = 0
        var streak = 0
    }

    private data class Abort(
        val reason: String,
        val unresolved: Int,
        val metric: String,
    )

    private data class FetchedAccounts(
        val reprtCode: String,
        val fsDiv: String,
        val accounts: List<DartFinancialAccount>,
    )

    private sealed interface Evaluation {
        data class Resolved(val row: FinancialSummaryRow) : Evaluation
        data object Absent : Evaluation
        data object Failed : Evaluation
    }

    private sealed interface BackfillOutcome {
        data class Done(val rows: List<FinancialSummaryRow>) : BackfillOutcome
        data object StockFailed : BackfillOutcome
        data object DeadlineReached : BackfillOutcome
    }

    private enum class ProcessResult { STORED, ABSENT, FAILED }

    companion object {
        const val JOB_NAME = "financials_sync"
        const val CONSOLIDATED = "CFS"
        const val SEPARATE = "OFS"
        const val HISTORY_COVERAGE_YEARS = 2
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val FATAL_STATUSES = setOf("010", "011", "012", "020", "021", "100", "101", "800", "900", "901")
    }
}
