package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.DigestData
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.envelope.StreamCategory
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.DigestClusterRow
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.MarketDigestStore
import com.alphatalk.worker.llm.persist.StreamEventRow
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

@Service
class DigestProcessor(
    private val store: ClusterStore,
    private val sectors: SectorDirectory,
    private val events: StreamEventStore,
    private val marketDigests: MarketDigestStore,
    private val publisher: StreamPublisher,
    private val eventIds: EventIdGenerator,
    private val llm: LlmClient,
    props: LlmProperties,
    private val zone: ZoneId = ZoneId.of("Asia/Seoul"),
    private val clock: Clock = Clock.systemUTC(),
) {
    private val limits = props.digest

    init {
        require(
            listOf(
                limits.positiveLimit,
                limits.negativeLimit,
                limits.neutralLimit,
                limits.sectorLimit,
                limits.marketLimit,
            ).all { it > 0 },
        ) { "다이제스트 입력 상한은 모두 양수여야 한다: $limits" }
    }

    fun process(entry: IngestQueueEntry) {
        val code = entry.codes.single()
        val date = entry.sourceId.substringAfterLast(':')
        if (events.digestExists(code, date)) return

        val windowTo = LocalDate.parse(date).atTime(LocalTime.of(18, 0)).atZone(zone).toInstant()
        val windowFrom = windowTo.minus(Duration.ofHours(24))

        val allStockRows = store.stockClustersInWindow(code, windowFrom, windowTo)
        val stockClusterIds = allStockRows.map { it.clusterId }.toSet()
        val allSectorRows = sectors.sectorOf(code)
            ?.let { store.sectorClustersInWindow(it, code, windowFrom, windowTo) }
            .orEmpty()
            .filter { it.clusterId !in stockClusterIds }
        val allMarketRows = store.marketClustersInWindow(windowFrom, windowTo)
            .filter { it.clusterId !in stockClusterIds }
        if (allStockRows.isEmpty() && allSectorRows.isEmpty()) return

        val positives = selectStockRows(allStockRows, Sentiment.POSITIVE, limits.positiveLimit)
        val negatives = selectStockRows(allStockRows, Sentiment.NEGATIVE, limits.negativeLimit)
        val neutrals = selectStockRows(allStockRows, Sentiment.NEUTRAL, limits.neutralLimit)
        val stockRows = positives + negatives + neutrals
        val sectorRows = allSectorRows.sortedWith(SECTOR_ORDER).take(limits.sectorLimit)
        val marketRows = allMarketRows.sortedByDescending(DigestClusterRow::lastArticleAt).take(limits.marketLimit)

        val stockName = sectors.stockName(code) ?: code
        val output = llm.digest(
            DigestInput(
                code = code,
                stockName = stockName,
                date = date,
                stockClusters = stockRows.map(::toDigestCluster),
                sectorClusters = sectorRows.map(::toDigestCluster),
                marketClusters = marketRows.map(::toDigestCluster),
            ),
        )

        val data = StreamData(
            category = StreamCategory.AI.payload,
            title = output.title,
            summary = output.summary,
            occurredAt = clock.millis(),
            digest = DigestData(
                date = date,
                positives = positives.map(::toItem),
                negatives = negatives.map(::toItem),
                sectorIssues = sectorRows.map {
                    DigestData.SectorIssue(
                        title = it.title,
                        line = firstLine(it.summary),
                        sentiment = it.sentiment ?: Sentiment.NEUTRAL.name,
                        eventId = it.streamEventId,
                    )
                },
                marketIssues = marketRows.map { DigestData.MarketIssue(title = it.title, line = firstLine(it.summary)) },
                marketAnalysis = runCatching { marketDigests.find(date) }.getOrNull(),
                inputCounts = DigestData.Counts(
                    stock = allStockRows.size,
                    sector = allSectorRows.size,
                    market = allMarketRows.size,
                ),
                includedCounts = DigestData.Counts(
                    stock = stockRows.size,
                    sector = sectorRows.size,
                    market = marketRows.size,
                ),
                pipelineVersion = PIPELINE_VERSION,
                neutralCount = allStockRows.count { it.sentiment == null || it.sentiment == Sentiment.NEUTRAL.name },
                newsCount = (allStockRows + allSectorRows + allMarketRows).sumOf { it.articleCount },
            ),
        )
        val eventId = eventIds.next()
        val row = StreamEventRow(eventId, code, StreamCategory.AI.eventType, clock.instant(), "worker-llm", data)
        if (eventId in events.insertEvents(listOf(row))) {
            publisher.publish(code, eventId, data)
        }
    }

    private fun toDigestCluster(row: DigestClusterRow) = DigestCluster(
        eventId = row.streamEventId,
        title = row.title,
        summary = row.summary,
        sentiment = row.sentiment?.let(Sentiment::valueOf) ?: Sentiment.NEUTRAL,
        articleCount = row.articleCount,
    )

    private fun toItem(row: DigestClusterRow) = DigestData.Item(
        title = row.title,
        line = firstLine(row.summary),
        eventId = row.streamEventId,
    )

    private fun firstLine(summary: String): String =
        summary.lineSequence().firstOrNull().orEmpty().take(80)

    private fun selectStockRows(rows: List<DigestClusterRow>, sentiment: Sentiment, limit: Int): List<DigestClusterRow> =
        rows.asSequence()
            .filter {
                if (sentiment == Sentiment.NEUTRAL) {
                    it.sentiment == null || it.sentiment == Sentiment.NEUTRAL.name
                } else {
                    it.sentiment == sentiment.name
                }
            }
            .sortedWith(STOCK_ORDER)
            .take(limit)
            .toList()

    private companion object {
        const val PIPELINE_VERSION = 2

        val STOCK_ORDER = compareByDescending<DigestClusterRow> { it.confidence ?: 0.0 }
            .thenByDescending(DigestClusterRow::lastArticleAt)

        val SECTOR_ORDER = compareBy<DigestClusterRow> {
            when (it.impact) {
                Impact.HIGH.name -> 0
                Impact.MEDIUM.name -> 1
                else -> 2
            }
        }
            .thenByDescending { it.confidence ?: 0.0 }
            .thenByDescending(DigestClusterRow::lastArticleAt)
    }
}
