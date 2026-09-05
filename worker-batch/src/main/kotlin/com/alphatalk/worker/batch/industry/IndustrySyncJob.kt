package com.alphatalk.worker.batch.industry

import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

open class IndustrySyncJob(
    private val dart: DartClient,
    private val ksic: KsicCatalog,
    private val store: IndustryStore,
    private val runs: BatchJobRunStore,
    private val meters: MeterRegistry,
    private val requestInterval: Duration = Duration.ofMillis(50),
    private val groupMaxSize: Int = 100,
    private val groupOverrides: Map<String, String> = emptyMap(),
    private val maxFailureRatio: Double = 0.05,
    private val failureStreakLimit: Int = 5,
    private val deadline: Duration = Duration.ofMinutes(90),
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
    private val pause: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        require(failureStreakLimit >= 1) {
            "alphatalk.batch.dart.failure-streak-limit는 1 이상이어야 한다: $failureStreakLimit"
        }
        require(deadline > Duration.ZERO) {
            "alphatalk.batch.dart.deadline은 양수여야 한다: $deadline"
        }
        require(deadline <= MAX_DEADLINE) {
            "alphatalk.batch.dart.deadline은 ShedLock 임대($LOCK_AT_MOST_FOR)에서 여유($DEADLINE_MARGIN)를 뺀 $MAX_DEADLINE 이하여야 한다: $deadline"
        }
    }

    @Scheduled(cron = "\${alphatalk.batch.dart.cron:0 30 6 * * SUN}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = LOCK_AT_MOST_FOR, lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    open fun syncOnce(): Int {
        val runDate = today().format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock()) ?: run {
            log.info("industry sync skipped, already succeeded: runDate={}", runDate)
            return 0
        }
        try {
            val deadlineAt = clock().plus(deadline)
            val listed = dart.corpCodes().filter { !it.stockCode.isNullOrBlank() }
            check(listed.isNotEmpty()) { "OpenDART corpCode 응답에 상장사가 없다" }
            store.upsertCorpMap(listed)

            val active = store.activeStockCodes()
            val targets = listed.filter { it.stockCode in active }
            val outcome = CollectOutcome()
            val abort = collectAll(outcome, targets, deadlineAt)
            if (abort != null) {
                meters.counter(abort.metric).increment()
                runs.failCounted(runId, 0, abort.unresolved, abort.reason, clock())
                log.error(
                    "industry sync aborted - marked FAILED, nothing stored: {} fetched={}",
                    abort.reason,
                    outcome.profiles.size,
                )
                return 0
            }
            checkFailureBudget(outcome, targets.size)

            val retired = active - targets.mapNotNull(DartCorp::stockCode).toSet() + outcome.missing
            val indutyCodes = store.activeIndutyCodes().toMutableMap()
            indutyCodes.keys.removeAll(retired)
            outcome.profiles.forEach { (code, company) -> company.indutyCode?.let { indutyCodes[code] = it } }
            val sectorCodes = assignSectors(indutyCodes)

            store.upsertSectors(catalogEntriesFor(sectorCodes.values.toSet()))
            val records = indutyCodes.map { (code, induty) ->
                val company = outcome.profiles[code]
                StockIndustryRecord(
                    code = code,
                    indutyCode = induty,
                    sectorCode = sectorCodes.getValue(code),
                    corpName = company?.corpName,
                    corpNameEng = company?.corpNameEng,
                    stockName = company?.stockName,
                    homepage = company?.homepage,
                )
            }
            val stored = store.upsertStockIndustries(records)
            val cleared = store.clearIndustryAssignments(active - indutyCodes.keys)

            meters.counter("batch.industry.synced").increment(stored.toDouble())
            meters.counter("batch.industry.failed").increment(outcome.failed.size.toDouble())
            runs.succeed(runId, stored, outcome.failed.size, clock())
            log.info(
                "industry sync done: corpMap={} targets={} fetched={} assigned={} cleared={} missing={} failed={}",
                listed.size, targets.size, outcome.profiles.size, stored, cleared,
                outcome.missing.size, outcome.failed.size,
            )
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun assignSectors(indutyCodes: Map<String, String>): Map<String, String> {
        var assigned = indutyCodes.mapValues { ksic.sectorCodeOf(it.value, KsicCatalog.BASE_LEVEL) }
        var level = KsicCatalog.BASE_LEVEL
        while (level < KsicCatalog.MAX_LEVEL) {
            val splittable = assigned.oversizedGroups().filterKeys { group ->
                assigned.any { (code, g) -> g == group && indutyCodes.getValue(code).length > group.length }
            }
            if (splittable.isEmpty()) break
            level += 1
            log.info("splitting oversized sector groups at level {}: {}", level, splittable)
            assigned = assigned.mapValues { (code, group) ->
                if (group in splittable) ksic.sectorCodeOf(indutyCodes.getValue(code), level) else group
            }
        }
        val overridden = applyOverrides(assigned)
        reportUnsplittable(overridden, indutyCodes)
        return overridden
    }

    private fun reportUnsplittable(assigned: Map<String, String>, indutyCodes: Map<String, String>) {
        val oversized = assigned.oversizedGroups()
        if (oversized.isEmpty()) return
        val splittable = oversized.filterKeys { group ->
            assigned.any { (code, g) -> g == group && indutyCodes.getValue(code).length > group.length }
        }
        check(splittable.isEmpty()) {
            "쪼갤 수 있는데도 fan-out 상한($groupMaxSize)을 넘는 그룹이 남는다: $splittable"
        }
        meters.counter("batch.industry.oversized").increment(oversized.size.toDouble())
        log.warn(
            "sector groups exceed fan-out cap({}) and cannot be split further — worker-llm이 실시간 발행을 억제한다: {}",
            groupMaxSize, oversized,
        )
    }

    private fun Map<String, String>.oversizedGroups(): Map<String, Int> =
        values.groupingBy { it }.eachCount().filterValues { it > groupMaxSize }

    private fun applyOverrides(assigned: Map<String, String>): Map<String, String> {
        if (groupOverrides.isEmpty()) return assigned
        groupOverrides.keys.filterNot { it in assigned }.forEach {
            log.warn("sector override targets a stock without an industry code: code={}", it)
        }
        return assigned.mapValues { (code, group) ->
            groupOverrides[code]?.also {
                if (it != group) log.info("sector overridden: code={} {} -> {}", code, group, it)
            } ?: group
        }
    }

    private fun catalogEntriesFor(sectorCodes: Set<String>): List<KsicEntry> {
        val fallbacks = sectorCodes.filterNot(ksic::contains).map { code ->
            val name = ksic.ancestorNameOf(code)
            if (name == null) {
                log.warn("sector code has no KSIC name and no ancestor: code={}", code)
            } else {
                log.info("sector code falls back to ancestor name: code={} name={}", code, name)
            }
            KsicEntry(code, name ?: code)
        }
        return ksic.entries() + fallbacks
    }

    private fun collectAll(outcome: CollectOutcome, targets: List<DartCorp>, deadlineAt: Instant): Abort? {
        val deferred = mutableListOf<DartCorp>()
        for ((index, corp) in targets.withIndex()) {
            val unresolved = deferred.size + targets.size - index
            (deadlineAbort(deadlineAt, unresolved) ?: breakerAbort(outcome, unresolved))?.let { return it }
            if (collect(corp, outcome) == Lookup.FAILED) deferred += corp
        }
        breakerAbort(outcome, deferred.size)?.let { return it }
        for ((index, corp) in deferred.withIndex()) {
            deadlineAbort(deadlineAt, outcome.failed.size + deferred.size - index)?.let { return it }
            if (collect(corp, outcome) == Lookup.FAILED) outcome.failed += corp
        }
        return null
    }

    private fun collect(corp: DartCorp, outcome: CollectOutcome): Lookup {
        val result = lookup(corp, outcome)
        outcome.streak = if (result == Lookup.FAILED) outcome.streak + 1 else 0
        return result
    }

    private fun lookup(corp: DartCorp, outcome: CollectOutcome): Lookup {
        val code = checkNotNull(corp.stockCode)
        pause(requestInterval)
        val company = try {
            dart.company(corp.corpCode)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw e
        } catch (e: DartApiException) {
            if (isFatal(e.status)) throw e
            log.warn("company lookup failed: corpCode={} status={}", corp.corpCode, e.status)
            return Lookup.FAILED
        } catch (e: Exception) {
            log.warn("company lookup failed: corpCode={}", corp.corpCode, e)
            return Lookup.FAILED
        }
        if (company?.indutyCode == null) {
            outcome.missing += code
            return Lookup.MISSING
        }
        outcome.profiles[code] = company
        return Lookup.RESOLVED
    }

    private fun deadlineAbort(deadlineAt: Instant, unresolved: Int): Abort? =
        if (clock() < deadlineAt) {
            null
        } else {
            Abort(
                reason = "deadline reached: unresolved=$unresolved",
                unresolved = unresolved,
                metric = "batch.industry.deadline",
            )
        }

    private fun breakerAbort(outcome: CollectOutcome, unresolved: Int): Abort? =
        if (outcome.streak < failureStreakLimit) {
            null
        } else {
            Abort(
                reason = "failure streak=${outcome.streak} - upstream outage suspected: unresolved=$unresolved",
                unresolved = unresolved,
                metric = "batch.industry.breaker",
            )
        }

    private fun isFatal(status: String): Boolean =
        status in FATAL_STATUSES || status.toIntOrNull()?.let { it == 429 || it in 500..599 } == true

    private fun checkFailureBudget(outcome: CollectOutcome, targetCount: Int) {
        if (targetCount == 0 || outcome.failed.isEmpty()) return
        val ratio = outcome.failed.size.toDouble() / targetCount
        check(ratio <= maxFailureRatio) {
            "OpenDART 조회 실패가 허용치를 넘는다: ${outcome.failed.size}/$targetCount (허용 ${maxFailureRatio})"
        }
        log.warn("industry sync tolerated lookup failures: {}/{}", outcome.failed.size, targetCount)
    }

    private class CollectOutcome {
        val profiles = mutableMapOf<String, DartCompany>()
        val missing = mutableSetOf<String>()
        val failed = mutableListOf<DartCorp>()
        var streak = 0
    }

    private data class Abort(
        val reason: String,
        val unresolved: Int,
        val metric: String,
    )

    private enum class Lookup { RESOLVED, MISSING, FAILED }

    companion object {
        const val JOB_NAME = "industry_sync"
        const val LOCK_AT_MOST_FOR = "PT2H"
        private val DEADLINE_MARGIN: Duration = Duration.ofMinutes(15)
        private val MAX_DEADLINE: Duration = Duration.parse(LOCK_AT_MOST_FOR).minus(DEADLINE_MARGIN)
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val FATAL_STATUSES = setOf("010", "011", "012", "020", "021", "100", "101", "800", "900", "901")
    }
}
