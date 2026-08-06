package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.MarketAnalysis
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.DigestClusterRow
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.persist.MarketDigestStore
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.time.Clock
import java.time.Duration
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

@Service
class MarketDigestProcessor(
    private val store: ClusterStore,
    private val facts: MarketFactSheetSource,
    private val digests: MarketDigestStore,
    private val llm: LlmClient,
    private val props: LlmProperties,
    private val meters: MeterRegistry,
    private val zone: ZoneId = ZoneId.of("Asia/Seoul"),
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun process(entry: IngestQueueEntry) {
        val date = entry.sourceId.substringAfterLast(':')
        if (digests.find(date)?.degraded == false) return

        val windowTo = LocalDate.parse(date).atTime(LocalTime.of(17, 40)).atZone(zone).toInstant()
        val windowFrom = windowTo.minus(Duration.ofHours(24))

        val factsResult = runCatching { facts.lookup(LocalDate.parse(date)) }
            .onFailure { recordLayerFailure("facts", it) }
        val clustersResult = runCatching {
            val market = store.marketClustersInWindow(windowFrom, windowTo)
            val sector = store.highImpactSectorClustersInWindow(windowFrom, windowTo)
                .filter { candidate -> market.none { it.clusterId == candidate.clusterId } }
            market to sector
        }.onFailure { recordLayerFailure("clusters", it) }

        val factSheet = (factsResult.getOrNull() as? FactSheetLookup.Found)?.sheet
        val (marketClusters, sectorClusters) = clustersResult.getOrDefault(emptyList<DigestClusterRow>() to emptyList())
        val researchExpected = props.market.researchEnabled && llm.supportsMarketResearch()
        val factsUnavailable = factsResult.isFailure ||
            factsResult.getOrNull() is FactSheetLookup.Insufficient

        if (factSheet == null && marketClusters.isEmpty() && sectorClusters.isEmpty() && !researchExpected) {
            check(!factsUnavailable && !clustersResult.isFailure) {
                "시장 다이제스트 입력 전 층이 실패·불완전한데 남은 내용이 없다 — 재시도 대상"
            }
            meters.counter("market.digest.skipped.empty").increment()
            log.info("market digest skipped: no input date={}", date)
            return
        }

        val asOf = clock.instant().atZone(zone).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
        val input = MarketDigestInput(
            date = date,
            asOf = asOf,
            factSheet = factSheet,
            marketClusters = marketClusters.map(::toDigestCluster),
            sectorClusters = sectorClusters.map(::toDigestCluster),
            research = researchExpected,
        )
        var usedResearch = researchExpected
        val rawOutput = runCatching { llm.marketDigest(input) }.getOrElse { failure ->
            if (!researchExpected) throw failure
            recordLayerFailure("research", failure)
            if (factSheet == null && marketClusters.isEmpty() && sectorClusters.isEmpty()) throw failure
            usedResearch = false
            llm.marketDigest(input.copy(research = false))
        }
        val output = if (usedResearch) rawOutput else rawOutput.copy(global = emptyList(), sources = emptyList())

        if (factSheet == null && marketClusters.isEmpty() && sectorClusters.isEmpty() && output.global.isEmpty()) {
            val researchFailed = researchExpected && !usedResearch
            check(!factsUnavailable && !clustersResult.isFailure && !researchFailed) {
                "시장 다이제스트 층 실패에 남은 내용이 없다 — ACK하면 멱등 마커가 그 날짜를 봉인하므로 재시도 대상"
            }
            meters.counter("market.digest.skipped.empty").increment()
            log.info("market digest skipped: research returned nothing and no other input date={}", date)
            return
        }

        val researched = usedResearch && output.global.isNotEmpty()
        val degraded = factsResult.getOrNull() !is FactSheetLookup.Found ||
            clustersResult.isFailure ||
            !researched
        val analysis = MarketAnalysis(
            summary = output.summary,
            domestic = output.domestic,
            global = output.global,
            sources = output.sources,
            asOf = asOf,
            factDate = factSheet?.factDate,
            degraded = degraded,
        )
        val saved = digests.save(date, analysis)
        meters.counter("market.digest.generated", "degraded", degraded.toString()).increment()
        log.info("market digest generated: date={} degraded={} saved={}", date, degraded, saved)
    }

    private fun toDigestCluster(row: DigestClusterRow) = DigestCluster(
        eventId = row.streamEventId,
        title = row.title,
        summary = row.summary,
        sentiment = row.sentiment?.let(Sentiment::valueOf) ?: Sentiment.NEUTRAL,
        articleCount = row.articleCount,
    )

    private fun recordLayerFailure(layer: String, failure: Throwable) {
        meters.counter("market.digest.layer.failed", "layer", layer).increment()
        log.warn("market digest layer failed: layer={}", layer, failure)
    }
}
