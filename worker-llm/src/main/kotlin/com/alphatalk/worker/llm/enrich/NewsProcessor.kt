package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.SectorRef
import com.alphatalk.contracts.envelope.SourceRef
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterContendedException
import com.alphatalk.worker.llm.cluster.ClusterRecord
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.SectorLinkWrite
import com.alphatalk.worker.llm.cluster.StockLink
import com.alphatalk.worker.llm.cluster.StockVerdictWrite
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.StreamEventRow
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration
import java.util.UUID

@Service
class NewsProcessor(
    private val store: ClusterStore,
    private val assigner: ClusterAssigner,
    private val fetcher: ArticleFetcher,
    private val summarizer: ClusterSummarizer,
    private val stockEvidence: StockEvidenceValidator,
    private val sectors: SectorDirectory,
    private val events: StreamEventStore,
    private val publisher: StreamPublisher,
    private val eventIds: EventIdGenerator,
    private val mapper: ObjectMapper,
    private val meters: MeterRegistry,
    private val transactions: TransactionRunner,
    props: LlmProperties,
    private val summarizeLease: Duration = Duration.ofMinutes(2),
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val fanoutCap: Int = props.sector.fanoutCap
    private val fanoutHardCap: Int = props.sector.fanoutHardCap

    init {
        require(fanoutCap > 0 && fanoutHardCap >= fanoutCap) {
            "fan-out 상한은 fanout-cap > 0 이고 fanout-hard-cap >= fanout-cap 이어야 한다: " +
                "cap=$fanoutCap hardCap=$fanoutHardCap"
        }
    }

    fun process(entry: IngestQueueEntry) {
        val assignment = assigner.assign(entry)
        val cluster = store.cluster(assignment.clusterId)
        if (assignment.joined) meters.counter("cluster.merged").increment()

        when (cluster.status) {
            ClusterStatus.NEW, ClusterStatus.SUMMARIZING -> {
                val token = UUID.randomUUID().toString()
                val claimedAt = clock.instant()
                if (!store.claimSummarize(cluster.id, token, claimedAt, claimedAt.minus(summarizeLease))) {
                    throw ClusterContendedException(cluster.id)
                }
                summarizeAndPublish(entry, cluster, token)
            }
            ClusterStatus.SUMMARIZED -> refreshAfterJoin(entry, cluster)
            ClusterStatus.IRRELEVANT -> Unit
        }
    }

    private fun summarizeAndPublish(entry: IngestQueueEntry, cluster: ClusterRecord, token: String) {
        val body = entry.url.takeIf { it.isNotBlank() }?.let(fetcher::fetchBody) ?: entry.body
        val summaryInput = ClusterSummaryInput(
            repTitle = cluster.repTitle,
            articleTitles = listOf(cluster.repTitle, entry.title).distinct(),
            body = body,
            stocks = entry.codes.map { StockCandidate(it, sectors.stockName(it) ?: it) },
            sectors = sectors.allSectors().map { SectorCandidate(it.code, it.name) },
        )
        val verdict = summarizer.summarize(summaryInput)
        ensureSummarizeLease(cluster.id, token)
        val plan = planPublication(entry, cluster.id, verdict, summaryInput)
        val publications = mutableListOf<PendingPublication>()
        transactions.run {
            ensureSummarizeLease(cluster.id, token)
            applyPlan(cluster, verdict.summary, plan, token, publications)
        }
        recordPlanMetrics(plan)
        publish(publications)
    }

    private fun planPublication(
        entry: IngestQueueEntry,
        clusterId: String,
        verdict: ClusterSummaryOutput,
        summaryInput: ClusterSummaryInput,
    ): SummaryPlan {
        if (!verdict.marketRelevant) return SummaryPlan()

        val sectorVerdicts = normalizeSectors(verdict.sectors)
        val candidates = entry.codes.toSet()
        val (relevantStocks, excludedStocks) = verdict.stocks
            .filter(StockVerdict::relevant)
            .partition { stockEvidence.accepts(it, candidates, summaryInput) }
        val evidenceRejected = excludedStocks.filter { it.relation == StockRelation.DIRECT }
        if (evidenceRejected.isNotEmpty()) {
            log.info(
                "stock evidence rejected: clusterId={} codes={}",
                clusterId,
                evidenceRejected.map(StockVerdict::code),
            )
        }
        val relevantCodes = relevantStocks.map { it.code }.toSet()
        val rejectedCandidates = entry.codes.filter { it !in relevantCodes }.map { code ->
            StockVerdictWrite(
                code = code,
                sentiment = null,
                confidence = verdict.stocks.firstOrNull { it.code == code }?.confidence,
                rejected = true,
            )
        }
        val base = SummaryPlan(
            sectorVerdicts = sectorVerdicts,
            stockLinkWrites = rejectedCandidates,
            evidenceRejected = evidenceRejected,
        )

        return when (verdict.scope) {
            NewsScope.STOCK ->
                if (relevantStocks.isNotEmpty()) {
                    base.copy(
                        finalScope = NewsScope.STOCK,
                        stockEvents = relevantStocks.distinctBy(StockVerdict::code).map {
                            PlannedStockEvent(it.code, it.sentiment.name, it.confidence, null, null)
                        },
                    )
                } else {
                    log.info("stock scope downgraded: clusterId={} sectors={}", clusterId, sectorVerdicts.size)
                    planSectorScope(clusterId, base, emptyList(), candidates).copy(downgradedFromStock = true)
                }
            NewsScope.SECTOR -> planSectorScope(clusterId, base, relevantStocks, candidates)
            NewsScope.MARKET -> base.copy(finalScope = NewsScope.MARKET)
        }
    }

    private fun planSectorScope(
        clusterId: String,
        base: SummaryPlan,
        relevantStocks: List<StockVerdict>,
        sourceCandidates: Set<String>,
    ): SummaryPlan {
        if (relevantStocks.isEmpty() && base.sectorVerdicts.isEmpty()) return base

        val (sourced, discovered) = relevantStocks.partition { it.code in sourceCandidates }
        val discoveredCodes = discovered.mapTo(mutableSetOf(), StockVerdict::code)
        val exemptCodes = sourced.mapTo(mutableSetOf(), StockVerdict::code)
        val outcome = resolveFanout(clusterId, base.sectorVerdicts, discoveredCodes, exemptCodes)

        val linkOnly = if (outcome.plan.includeDiscovered) {
            emptyList()
        } else {
            discovered.map { StockVerdictWrite(it.code, it.sentiment.name, it.confidence, rejected = false) }
        }
        val capExempt = sourced + if (outcome.plan.includeDiscovered) discovered else emptyList()
        val stockEvents = (
            capExempt.map {
                PlannedStockEvent(it.code, it.sentiment.name, it.confidence, NewsScope.SECTOR, sectorRefOf(it.code))
            } +
                outcome.plan.members.map { (code, context) ->
                    val (sectorVerdict, ref) = context
                    PlannedStockEvent(
                        code,
                        sectorVerdict.sentiment.name,
                        sectorVerdict.confidence,
                        NewsScope.SECTOR,
                        ref,
                    )
                }
            ).distinctBy(PlannedStockEvent::code)

        return base.copy(
            finalScope = NewsScope.SECTOR,
            stockLinkWrites = base.stockLinkWrites + linkOnly,
            stockEvents = stockEvents,
            fanoutCounters = outcome.counters,
        )
    }

    private fun applyPlan(
        cluster: ClusterRecord,
        summary: String,
        plan: SummaryPlan,
        token: String,
        publications: MutableList<PendingPublication>,
    ) {
        store.upsertSectorLinks(
            cluster.id,
            plan.sectorVerdicts.map {
                SectorLinkWrite(it.sectorCode, it.sentiment.name, it.confidence, it.impact.name)
            },
        )
        store.applyStockVerdicts(
            cluster.id,
            plan.stockLinkWrites + plan.stockEvents.map {
                StockVerdictWrite(it.code, it.sentiment, it.confidence, rejected = false)
            },
        )
        if (plan.stockEvents.isNotEmpty()) {
            publications += persistStockEvents(
                cluster = cluster,
                summary = summary,
                plannedEvents = plan.stockEvents,
                sources = store.articleSources(cluster.id),
                sourceUrl = store.representativeUrl(cluster.id),
                existingEventIds = store.stockLinks(cluster.id).associate { it.code to it.streamEventId },
            )
        }
        if (plan.finalScope == null) {
            if (!store.markIrrelevant(cluster.id, token)) throw ClusterContendedException(cluster.id)
        } else if (!store.markSummarized(cluster.id, token, summary, plan.finalScope.name)) {
            throw ClusterContendedException(cluster.id)
        }
    }

    private fun normalizeSectors(verdicts: List<SectorVerdict>): List<SectorVerdict> =
        verdicts.sortedBy { it.impact.ordinal }.distinctBy(SectorVerdict::sectorCode)

    private fun resolveFanout(
        clusterId: String,
        verdicts: List<SectorVerdict>,
        discovered: Set<String>,
        exemptCodes: Set<String>,
    ): FanoutOutcome {
        val all = memberFanout(verdicts)
        val allCount = cappedCount(all, discovered, exemptCodes)
        if (allCount <= fanoutCap) return FanoutOutcome(FanoutPlan(all, includeDiscovered = true), emptyList())

        val counters = mutableListOf("sector.fanout.tier2")
        val material = all.filterValues { it.first.impact != Impact.LOW }
        val materialCount = cappedCount(material, discovered, exemptCodes)
        if ((material.isEmpty() && discovered.isEmpty()) || materialCount > fanoutHardCap) {
            counters += "sector.fanout.suppressed"
            log.warn(
                "sector fan-out suppressed: clusterId={} sectors={} all={} material={} discovered={} " +
                    "cap={} hardCap={}",
                clusterId, sectorCodesOf(verdicts), allCount, materialCount, discovered.size, fanoutCap, fanoutHardCap,
            )
            return FanoutOutcome(FanoutPlan(emptyMap(), includeDiscovered = false), counters)
        }
        if (materialCount < allCount) {
            counters += "sector.fanout.degraded"
            log.info(
                "sector fan-out degraded to material impact: clusterId={} sectors={} all={} material={} discovered={}",
                clusterId, sectorCodesOf(verdicts), allCount, materialCount, discovered.size,
            )
        }
        return FanoutOutcome(FanoutPlan(material, includeDiscovered = true), counters)
    }

    private fun cappedCount(
        members: Map<String, Pair<SectorVerdict, SectorRef>>,
        discovered: Set<String>,
        exemptCodes: Set<String>,
    ): Int = ((members.keys - exemptCodes) + discovered).size

    private data class FanoutPlan(
        val members: Map<String, Pair<SectorVerdict, SectorRef>>,
        val includeDiscovered: Boolean,
    )

    private data class FanoutOutcome(val plan: FanoutPlan, val counters: List<String>)

    private fun sectorCodesOf(verdicts: List<SectorVerdict>): String =
        verdicts.take(LOGGED_SECTORS).joinToString(",") { "${it.sectorCode}:${it.impact}" } +
            if (verdicts.size > LOGGED_SECTORS) ",…(+${verdicts.size - LOGGED_SECTORS})" else ""

    private fun memberFanout(verdicts: List<SectorVerdict>): Map<String, Pair<SectorVerdict, SectorRef>> {
        val fanout = linkedMapOf<String, Pair<SectorVerdict, SectorRef>>()
        verdicts.forEach { sv ->
            val ref = SectorRef(code = sv.sectorCode, name = sectors.sectorName(sv.sectorCode) ?: sv.sectorCode)
            sectors.memberCodes(sv.sectorCode).forEach { code -> fanout.putIfAbsent(code, sv to ref) }
        }
        return fanout
    }

    private fun sectorRefOf(code: String): SectorRef? =
        sectors.sectorOf(code)?.let { SectorRef(code = it, name = sectors.sectorName(it) ?: it) }

    private fun persistStockEvents(
        cluster: ClusterRecord,
        summary: String,
        plannedEvents: List<PlannedStockEvent>,
        sources: List<SourceRef>,
        sourceUrl: String?,
        existingEventIds: Map<String, String?>,
    ): List<PendingPublication> {
        val proposed = plannedEvents
            .filter { existingEventIds[it.code] == null }
            .associate { it.code to eventIds.next() }
        val claimed = store.claimStockEvents(cluster.id, proposed)

        val eventIdByCode = buildMap {
            plannedEvents.forEach { event ->
                val eventId = existingEventIds[event.code]
                    ?: proposed[event.code]?.takeIf { event.code in claimed }
                if (eventId != null) put(event.code, eventId)
            }
            val unresolved = plannedEvents.filterNot { it.code in keys }
            if (unresolved.isNotEmpty()) {
                val refreshed = store.stockLinks(cluster.id).associate { it.code to it.streamEventId }
                unresolved.forEach { event -> refreshed[event.code]?.let { put(event.code, it) } }
            }
        }

        val category = cluster.category
        val rows = plannedEvents.mapNotNull { event ->
            val eventId = eventIdByCode[event.code] ?: return@mapNotNull null
            event to StreamEventRow(
                eventId = eventId,
                code = event.code,
                type = category.eventType,
                occurredAt = clock.instant(),
                source = "worker-llm",
                data = StreamData(
                    category = category.payload,
                    title = cluster.repTitle,
                    summary = summary,
                    sourceUrl = sourceUrl,
                    occurredAt = clock.millis(),
                    sentiment = event.sentiment,
                    scope = event.scope?.name,
                    sector = event.sector,
                    sources = sources,
                ),
            )
        }
        val inserted = events.insertEvents(rows.map { it.second })
        return rows.filter { it.second.eventId in inserted }
            .map { (event, row) -> PendingPublication(event.code, row.eventId, row.data) }
    }

    private fun refreshAfterJoin(entry: IngestQueueEntry, cluster: ClusterRecord) {
        val sectorScoped = cluster.scope == NewsScope.SECTOR.name
        val sectorRefs = if (sectorScoped) entry.codes.associateWith(::sectorRefOf) else emptyMap()
        val publications = mutableListOf<PendingPublication>()
        transactions.run {
            val sources = store.articleSources(cluster.id)
            val links = store.stockLinks(cluster.id)
            events.refreshSources(links.mapNotNull(StockLink::streamEventId), mapper.writeValueAsString(sources))

            val published = links.filter { it.streamEventId != null }.map { it.code }.toSet()
            val rejected = links.filter { it.rejected == true }.map { it.code }.toSet()
            val linkByCode = links.associateBy(StockLink::code)
            val plannedEvents = entry.codes
                .filter { it !in published && it !in rejected }
                .map { code ->
                    val existing = linkByCode[code]
                    PlannedStockEvent(
                        code = code,
                        sentiment = existing?.sentiment,
                        confidence = existing?.confidence,
                        scope = if (sectorScoped) NewsScope.SECTOR else null,
                        sector = sectorRefs[code],
                    )
                }
            if (plannedEvents.isNotEmpty()) {
                store.applyStockVerdicts(
                    cluster.id,
                    plannedEvents.map { StockVerdictWrite(it.code, it.sentiment, it.confidence, rejected = false) },
                )
                publications += persistStockEvents(
                    cluster = cluster,
                    summary = cluster.summary.orEmpty(),
                    plannedEvents = plannedEvents,
                    sources = sources,
                    sourceUrl = store.representativeUrl(cluster.id),
                    existingEventIds = links.associate { it.code to it.streamEventId },
                )
            }
        }
        publish(publications)
    }

    private fun ensureSummarizeLease(clusterId: String, token: String) {
        if (!store.renewSummarize(clusterId, token, clock.instant())) {
            throw ClusterContendedException(clusterId)
        }
    }

    private fun recordPlanMetrics(plan: SummaryPlan) {
        if (plan.evidenceRejected.isNotEmpty()) {
            meters.counter("stock.evidence.rejected").increment(plan.evidenceRejected.size.toDouble())
        }
        if (plan.downgradedFromStock) {
            meters.counter("stock.scope.downgraded").increment()
        }
        plan.fanoutCounters.forEach { meters.counter(it).increment() }
    }

    private fun publish(publications: List<PendingPublication>) {
        publications.forEach { publisher.publish(it.code, it.eventId, it.data) }
    }

    private companion object {
        const val LOGGED_SECTORS = 10
    }

    private data class SummaryPlan(
        val finalScope: NewsScope? = null,
        val sectorVerdicts: List<SectorVerdict> = emptyList(),
        val stockLinkWrites: List<StockVerdictWrite> = emptyList(),
        val stockEvents: List<PlannedStockEvent> = emptyList(),
        val evidenceRejected: List<StockVerdict> = emptyList(),
        val fanoutCounters: List<String> = emptyList(),
        val downgradedFromStock: Boolean = false,
    )

    private data class PlannedStockEvent(
        val code: String,
        val sentiment: String?,
        val confidence: Double?,
        val scope: NewsScope?,
        val sector: SectorRef?,
    )

    private data class EventContext(
        val sources: List<SourceRef>,
        val sourceUrl: String?,
        val eventIdByCode: Map<String, String?>,
    )

    private data class PendingPublication(
        val code: String,
        val eventId: String,
        val data: StreamData,
    )
}
