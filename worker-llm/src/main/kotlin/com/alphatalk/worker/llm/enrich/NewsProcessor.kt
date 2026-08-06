package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.SectorRef
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterContendedException
import com.alphatalk.worker.llm.cluster.ClusterRecord
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.persist.EventIdGenerator
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
        val verdict = summarizer.summarize(
            ClusterSummaryInput(
                repTitle = cluster.repTitle,
                articleTitles = listOf(cluster.repTitle, entry.title).distinct(),
                body = body,
                stocks = entry.codes.map { StockCandidate(it, sectors.stockName(it) ?: it) },
                sectors = sectors.allSectors().map { SectorCandidate(it.code, it.name) },
            ),
        )
        val publications = mutableListOf<PendingPublication>()
        transactions.run {
            ensureSummarizeLease(cluster.id, token)
            if (!verdict.marketRelevant) {
                if (!store.markIrrelevant(cluster.id, token)) throw ClusterContendedException(cluster.id)
                return@run
            }
            val sectorVerdicts = normalizeSectors(verdict.sectors)
            sectorVerdicts.forEach {
                store.upsertSectorLink(cluster.id, it.sectorCode, it.sentiment.name, it.confidence, it.impact.name)
            }

            val candidates = entry.codes.toSet()
            val relevantStocks = verdict.stocks
                .filter { it.relevant }
                .filter { it.code in candidates || sectors.stockName(it.code) != null }
            val relevantCodes = relevantStocks.map { it.code }.toSet()
            entry.codes.filter { it !in relevantCodes }.forEach { code ->
                val confidence = verdict.stocks.firstOrNull { it.code == code }?.confidence
                store.applyStockVerdict(cluster.id, code, null, confidence, rejected = true)
            }
            val finalScope = when (verdict.scope) {
                NewsScope.STOCK -> persistStockScope(cluster, verdict, relevantStocks, publications)
                NewsScope.SECTOR ->
                    persistSectorScope(cluster, verdict, sectorVerdicts, relevantStocks, publications)
                NewsScope.MARKET -> NewsScope.MARKET
            }
            if (finalScope == null) {
                if (!store.markIrrelevant(cluster.id, token)) throw ClusterContendedException(cluster.id)
            } else if (!store.markSummarized(cluster.id, token, verdict.summary, finalScope.name)) {
                throw ClusterContendedException(cluster.id)
            }
        }
        publish(publications)
    }

    private fun persistStockScope(
        cluster: ClusterRecord,
        verdict: ClusterSummaryOutput,
        relevantStocks: List<StockVerdict>,
        publications: MutableList<PendingPublication>,
    ): NewsScope? {
        if (relevantStocks.isEmpty()) return null
        relevantStocks.forEach { stock ->
            persistStockEvent(cluster, verdict.summary, stock.code, stock.sentiment.name, stock.confidence, null, null)
                ?.let(publications::add)
        }
        return NewsScope.STOCK
    }

    private fun persistSectorScope(
        cluster: ClusterRecord,
        verdict: ClusterSummaryOutput,
        sectorVerdicts: List<SectorVerdict>,
        relevantStocks: List<StockVerdict>,
        publications: MutableList<PendingPublication>,
    ): NewsScope? {
        if (relevantStocks.isEmpty() && sectorVerdicts.isEmpty()) return null
        val fanout = resolveFanout(cluster.id, sectorVerdicts)

        val direct = relevantStocks.map { it.code }.toSet()
        relevantStocks.forEach { stock ->
            persistStockEvent(
                cluster,
                verdict.summary,
                stock.code,
                stock.sentiment.name,
                stock.confidence,
                NewsScope.SECTOR,
                sectorRefOf(stock.code),
            )?.let(publications::add)
        }
        fanout.filterKeys { it !in direct }.forEach { (code, ctx) ->
            val (sv, ref) = ctx
            persistStockEvent(cluster, verdict.summary, code, sv.sentiment.name, sv.confidence, NewsScope.SECTOR, ref)
                ?.let(publications::add)
        }
        return NewsScope.SECTOR
    }

    private fun normalizeSectors(verdicts: List<SectorVerdict>): List<SectorVerdict> =
        verdicts.sortedBy { it.impact.ordinal }.distinctBy(SectorVerdict::sectorCode)

    private fun resolveFanout(
        clusterId: String,
        verdicts: List<SectorVerdict>,
    ): Map<String, Pair<SectorVerdict, SectorRef>> {
        val all = memberFanout(verdicts)
        if (all.size <= fanoutCap) return all

        val material = all.filterValues { it.first.impact != Impact.LOW }
        if (material.isEmpty() || material.size > fanoutHardCap) {
            meters.counter("sector.fanout.suppressed").increment()
            log.warn(
                "sector fan-out suppressed: clusterId={} sectors={} all={} material={} cap={} hardCap={}",
                clusterId, sectorCodesOf(verdicts), all.size, material.size, fanoutCap, fanoutHardCap,
            )
            return emptyMap()
        }
        if (material.size < all.size) {
            meters.counter("sector.fanout.degraded").increment()
            log.info(
                "sector fan-out degraded to material impact: clusterId={} sectors={} all={} material={}",
                clusterId, sectorCodesOf(verdicts), all.size, material.size,
            )
        }
        return material
    }

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

    private fun persistStockEvent(
        cluster: ClusterRecord,
        summary: String,
        code: String,
        sentiment: String?,
        confidence: Double?,
        scope: NewsScope?,
        sector: SectorRef?,
    ): PendingPublication? {
        store.applyStockVerdict(cluster.id, code, sentiment, confidence, rejected = false)
        val eventId = store.stockLinks(cluster.id).first { it.code == code }.streamEventId
            ?: eventIds.next().takeIf { store.claimStockEvent(cluster.id, code, it) }
            ?: store.stockLinks(cluster.id).first { it.code == code }.streamEventId
            ?: return null

        val category = cluster.category
        val data = StreamData(
            category = category.payload,
            title = cluster.repTitle,
            summary = summary,
            sourceUrl = store.representativeUrl(cluster.id),
            occurredAt = clock.millis(),
            sentiment = sentiment,
            scope = scope?.name,
            sector = sector,
            sources = store.articleSources(cluster.id),
        )
        return PendingPublication(code, eventId, data)
            .takeIf { events.insertEvent(eventId, code, category.eventType, clock.instant(), "worker-llm", data) }
    }

    private fun refreshAfterJoin(entry: IngestQueueEntry, cluster: ClusterRecord) {
        val publications = mutableListOf<PendingPublication>()
        transactions.run {
            val sourcesJson = mapper.writeValueAsString(store.articleSources(cluster.id))
            val links = store.stockLinks(cluster.id)
            links.mapNotNull { it.streamEventId }.forEach { events.refreshSources(it, sourcesJson) }

            val published = links.filter { it.streamEventId != null }.map { it.code }.toSet()
            val rejected = links.filter { it.rejected == true }.map { it.code }.toSet()
            entry.codes.filter { it !in published && it !in rejected }.forEach { code ->
                persistStockEvent(cluster, cluster.summary.orEmpty(), code, null, null, null, null)
                    ?.let(publications::add)
            }
        }
        publish(publications)
    }

    private fun ensureSummarizeLease(clusterId: String, token: String) {
        if (!store.renewSummarize(clusterId, token, clock.instant())) {
            throw ClusterContendedException(clusterId)
        }
    }

    private fun publish(publications: List<PendingPublication>) {
        publications.forEach { publisher.publish(it.code, it.eventId, it.data) }
    }

    private companion object {
        const val LOGGED_SECTORS = 10
    }

    private data class PendingPublication(
        val code: String,
        val eventId: String,
        val data: StreamData,
    )
}
