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
    private val clock: () -> Instant = Instant::now,
    private val today: () -> LocalDate = { LocalDate.now(SEOUL) },
    private val pause: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.dart.cron:0 30 6 * * SUN}", zone = "Asia/Seoul")
    @SchedulerLock(name = JOB_NAME, lockAtMostFor = "PT2H", lockAtLeastFor = "PT1M")
    open fun scheduled() {
        syncOnce()
    }

    fun syncOnce(): Int {
        val runDate = today().format(DateTimeFormatter.BASIC_ISO_DATE)
        val runId = runs.start(JOB_NAME, runDate, clock()) ?: run {
            log.info("industry sync skipped, already succeeded: runDate={}", runDate)
            return 0
        }
        try {
            val catalog = store.upsertIndustries(ksic.entries())
            val listed = dart.corpCodes().filter { !it.stockCode.isNullOrBlank() }
            check(listed.isNotEmpty()) { "OpenDART corpCode 응답에 상장사가 없다" }
            store.upsertCorpMap(listed)

            val active = store.activeStockCodes()
            val targets = listed.filter { it.stockCode in active }
            val collected = mutableListOf<StockIndustryRecord>()
            val failed = mutableListOf<DartCorp>()
            targets.forEach { corp -> fetch(corp)?.let(collected::add) ?: failed.add(corp) }
            failed.toList().forEach { corp ->
                fetch(corp)?.let {
                    collected += it
                    failed -= corp
                }
            }

            val grouped = applyOverrides(regroupOversized(collected))
            store.upsertIndustries(missingCatalogEntries(grouped))
            val stored = store.upsertStockIndustries(grouped)
            meters.counter("batch.industry.synced").increment(stored.toDouble())
            meters.counter("batch.industry.failed").increment(failed.size.toDouble())
            runs.succeed(runId, stored, failed.size, clock())
            log.info(
                "industry sync done: catalog={} corpMap={} targets={} stored={} failed={}",
                catalog, listed.size, targets.size, stored, failed.size,
            )
            return stored
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun applyOverrides(records: List<StockIndustryRecord>): List<StockIndustryRecord> {
        if (groupOverrides.isEmpty()) return records
        val applied = records.map { record ->
            groupOverrides[record.code]?.takeIf { it != record.groupCode }?.let { group ->
                log.info("industry group overridden: code={} {} -> {}", record.code, record.groupCode, group)
                record.copy(groupCode = group)
            } ?: record
        }
        val codes = records.mapTo(mutableSetOf(), StockIndustryRecord::code)
        groupOverrides.keys.filterNot { it in codes }.forEach {
            log.warn("industry group override targets an uncollected stock: code={}", it)
        }
        return applied
    }

    private fun missingCatalogEntries(records: List<StockIndustryRecord>): List<KsicEntry> {
        val known = ksic.entries().mapTo(mutableSetOf(), KsicEntry::code)
        return records.map(StockIndustryRecord::groupCode)
            .distinct()
            .filter { it !in known }
            .map { group ->
                val name = ksic.ancestorNameOf(group)
                if (name == null) {
                    log.warn("industry group has no KSIC name and no ancestor: group={}", group)
                } else {
                    log.info("industry group falls back to ancestor name: group={} name={}", group, name)
                }
                KsicEntry(group, name ?: group)
            }
    }

    private fun regroupOversized(records: List<StockIndustryRecord>): List<StockIndustryRecord> {
        val oversized = records.groupingBy(StockIndustryRecord::groupCode).eachCount()
            .filterValues { it > groupMaxSize }
        if (oversized.isEmpty()) return records
        log.info("splitting oversized industry groups: {}", oversized)
        val split = records.map {
            if (it.groupCode in oversized.keys) it.copy(groupCode = ksic.subGroupCodeOf(it.indutyCode)) else it
        }
        split.groupingBy(StockIndustryRecord::groupCode).eachCount()
            .filterValues { it > groupMaxSize }
            .forEach { (group, size) ->
                log.warn("industry group still exceeds fan-out cap after split: group={} size={}", group, size)
            }
        return split
    }

    private fun fetch(corp: DartCorp): StockIndustryRecord? {
        pause(requestInterval)
        val company = try {
            dart.company(corp.corpCode)
        } catch (e: Exception) {
            log.warn("company lookup failed: corpCode={} corpName={}", corp.corpCode, corp.corpName, e)
            null
        } ?: return null
        val induty = company.indutyCode?.takeIf { it.isNotBlank() } ?: return null
        val code = corp.stockCode ?: return null
        return StockIndustryRecord(
            code = code,
            indutyCode = induty,
            groupCode = ksic.groupCodeOf(induty),
            corpName = company.corpName,
            corpNameEng = company.corpNameEng,
            stockName = company.stockName,
            homepage = company.homepage,
        )
    }

    companion object {
        const val JOB_NAME = "industry_sync"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
