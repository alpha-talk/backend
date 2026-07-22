package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.DigestData
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.DigestClusterRow
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId

class DigestProcessor(
    private val store: ClusterStore,
    private val sectors: SectorDirectory,
    private val events: StreamEventStore,
    private val publisher: StreamPublisher,
    private val eventIds: EventIdGenerator,
    private val llm: LlmClient,
    private val zone: ZoneId = ZoneId.of("Asia/Seoul"),
    private val clock: Clock = Clock.systemUTC(),
) {
    fun process(entry: IngestQueueEntry) {
        val code = entry.codes.single()
        val date = entry.sourceId.substringAfterLast(':')
        if (events.digestExists(code, date)) return

        val windowTo = LocalDate.parse(date).atTime(LocalTime.of(18, 0)).atZone(zone).toInstant()
        val windowFrom = windowTo.minus(Duration.ofHours(24))

        val stockRows = store.stockClustersInWindow(code, windowFrom, windowTo)
        val stockClusterIds = stockRows.map { it.clusterId }.toSet()
        val sectorRows = sectors.sectorOf(code)
            ?.let { store.sectorClustersInWindow(it, code, windowFrom, windowTo) }
            .orEmpty()
            .filter { it.clusterId !in stockClusterIds }
        val marketRows = store.marketClustersInWindow(windowFrom, windowTo)
            .filter { it.clusterId !in stockClusterIds }
        if (stockRows.isEmpty() && sectorRows.isEmpty()) return

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
            category = "ai",
            title = output.title,
            summary = output.summary,
            occurredAt = clock.millis(),
            digest = DigestData(
                date = date,
                positives = stockRows.filter { it.sentiment == Sentiment.POSITIVE.name }.map(::toItem),
                negatives = stockRows.filter { it.sentiment == Sentiment.NEGATIVE.name }.map(::toItem),
                sectorIssues = sectorRows.map {
                    DigestData.SectorIssue(
                        title = it.title,
                        line = firstLine(it.summary),
                        sentiment = it.sentiment ?: Sentiment.NEUTRAL.name,
                        eventId = it.streamEventId,
                    )
                },
                marketIssues = marketRows.map { DigestData.MarketIssue(title = it.title, line = firstLine(it.summary)) },
                neutralCount = stockRows.count { it.sentiment == null || it.sentiment == Sentiment.NEUTRAL.name },
                newsCount = (stockRows + sectorRows + marketRows).sumOf { it.articleCount },
            ),
        )
        val eventId = eventIds.next()
        if (events.insertEvent(eventId, code, "AI", clock.instant(), "worker-llm", data)) {
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
}
