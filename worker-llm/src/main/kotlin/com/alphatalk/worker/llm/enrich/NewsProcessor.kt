package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.SectorRef
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.cluster.AssignResult
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterRecord
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import java.time.Clock

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
    private val fanoutCap: Int,
    private val coverageStocks: List<String>,
    private val clock: Clock = Clock.systemUTC(),
) {
    fun process(entry: IngestQueueEntry) {
        val existing = store.findArticleCluster(entry.sourceId)
        if (existing != null && existing.status != ClusterStatus.NEW) return

        val assignment = if (existing != null) {
            AssignResult(existing.id, joined = false, created = false)
        } else {
            assigner.assign(entry)
        }
        if (assignment.joined) meters.counter("cluster.merged").increment()

        val cluster = store.cluster(assignment.clusterId)
        when (cluster.status) {
            ClusterStatus.NEW -> summarizeAndPublish(entry, cluster)
            ClusterStatus.SUMMARIZED -> refreshAfterJoin(entry, cluster)
            ClusterStatus.IRRELEVANT -> Unit
        }
    }

    private fun summarizeAndPublish(entry: IngestQueueEntry, cluster: ClusterRecord) {
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
        verdict.sectors.forEach {
            store.upsertSectorLink(cluster.id, it.sectorCode, it.sentiment.name, it.confidence, it.impact.name)
        }

        val relevantStocks = verdict.stocks.filter { it.relevant }
        if (relevantStocks.isEmpty() && verdict.sectors.isEmpty() && verdict.scope != NewsScope.MARKET) {
            store.markIrrelevant(cluster.id)
            return
        }

        val finalScope = when (verdict.scope) {
            NewsScope.STOCK -> publishStockScope(cluster, verdict, relevantStocks)
            NewsScope.SECTOR -> publishSectorScope(cluster, verdict, relevantStocks)
            NewsScope.MARKET -> NewsScope.MARKET
        } ?: return
        store.markSummarized(cluster.id, verdict.summary, finalScope.name)
    }

    private fun publishStockScope(
        cluster: ClusterRecord,
        verdict: ClusterSummaryOutput,
        relevantStocks: List<StockVerdict>,
    ): NewsScope? {
        if (relevantStocks.isEmpty()) {
            store.markIrrelevant(cluster.id)
            return null
        }
        relevantStocks.forEach { stock ->
            publishStockEvent(
                cluster = cluster,
                summary = verdict.summary,
                code = stock.code,
                sentiment = stock.sentiment.name,
                confidence = stock.confidence,
                scope = null,
                sector = null,
            )
        }
        return NewsScope.STOCK
    }

    private fun publishSectorScope(
        cluster: ClusterRecord,
        verdict: ClusterSummaryOutput,
        relevantStocks: List<StockVerdict>,
    ): NewsScope {
        val fanout = linkedMapOf<String, Pair<SectorVerdict, SectorRef>>()
        verdict.sectors.filter { it.impact != Impact.LOW }.forEach { sv ->
            val members = sectors.memberCodes(sv.sectorCode)
            val covered = if (coverageStocks.isEmpty()) members else members.filter { it in coverageStocks }
            val ref = SectorRef(code = sv.sectorCode, name = sectors.sectorName(sv.sectorCode) ?: sv.sectorCode)
            covered.forEach { code -> fanout.putIfAbsent(code, sv to ref) }
        }
        if (fanout.size > fanoutCap) return NewsScope.MARKET

        relevantStocks.forEach { stock ->
            publishStockEvent(cluster, verdict.summary, stock.code, stock.sentiment.name, stock.confidence, null, null)
        }
        val direct = relevantStocks.map { it.code }.toSet()
        fanout.filterKeys { it !in direct }.forEach { (code, ctx) ->
            val (sv, ref) = ctx
            publishStockEvent(cluster, verdict.summary, code, sv.sentiment.name, sv.confidence, NewsScope.SECTOR, ref)
        }
        return NewsScope.SECTOR
    }

    private fun publishStockEvent(
        cluster: ClusterRecord,
        summary: String,
        code: String,
        sentiment: String?,
        confidence: Double?,
        scope: NewsScope?,
        sector: SectorRef?,
    ) {
        store.insertStockLinkIfAbsent(cluster.id, code, sentiment, confidence)
        val link = store.stockLinks(cluster.id).first { it.code == code }
        if (link.streamEventId != null) return

        val eventId = eventIds.next()
        val data = StreamData(
            category = "news",
            title = cluster.repTitle,
            summary = summary,
            sourceUrl = store.representativeUrl(cluster.id),
            occurredAt = clock.millis(),
            sentiment = link.sentiment ?: sentiment,
            scope = scope?.name,
            sector = sector,
            sources = store.articleSources(cluster.id),
        )
        if (events.insertEvent(eventId, code, "NEWS", clock.instant(), "worker-llm", data)) {
            store.setStockLinkEvent(cluster.id, code, eventId)
            publisher.publish(code, eventId, data)
        }
    }

    private fun refreshAfterJoin(entry: IngestQueueEntry, cluster: ClusterRecord) {
        val sourcesJson = mapper.writeValueAsString(store.articleSources(cluster.id))
        val links = store.stockLinks(cluster.id)
        links.mapNotNull { it.streamEventId }.forEach { events.refreshSources(it, sourcesJson) }

        val known = links.map { it.code }.toSet()
        entry.codes.filter { it !in known }.forEach { code ->
            publishStockEvent(
                cluster = cluster,
                summary = cluster.summary.orEmpty(),
                code = code,
                sentiment = null,
                confidence = null,
                scope = null,
                sector = null,
            )
        }
    }
}
