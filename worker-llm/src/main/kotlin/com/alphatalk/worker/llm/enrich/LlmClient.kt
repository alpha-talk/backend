package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment

interface LlmClient {
    fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput
    fun digest(input: DigestInput): DigestOutput
}

data class StockCandidate(val code: String, val name: String)

data class SectorCandidate(val code: String, val name: String)

data class ClusterSummaryInput(
    val repTitle: String,
    val articleTitles: List<String>,
    val body: String?,
    val stocks: List<StockCandidate>,
    val sectors: List<SectorCandidate>,
)

data class ClusterSummaryOutput(
    val summary: String,
    val scope: NewsScope,
    val stocks: List<StockVerdict>,
    val sectors: List<SectorVerdict>,
)

data class StockVerdict(
    val code: String,
    val relevant: Boolean,
    val sentiment: Sentiment,
    val confidence: Double,
    val reason: String,
)

data class SectorVerdict(
    val sectorCode: String,
    val sentiment: Sentiment,
    val impact: Impact,
    val confidence: Double,
    val reason: String,
)

enum class Impact { HIGH, MEDIUM, LOW }

data class DigestInput(
    val code: String,
    val stockName: String,
    val date: String,
    val stockClusters: List<DigestCluster>,
    val sectorClusters: List<DigestCluster>,
    val marketClusters: List<DigestCluster>,
)

data class DigestCluster(
    val eventId: String?,
    val title: String,
    val summary: String,
    val sentiment: Sentiment,
    val articleCount: Int,
)

data class DigestOutput(
    val title: String,
    val summary: String,
)
