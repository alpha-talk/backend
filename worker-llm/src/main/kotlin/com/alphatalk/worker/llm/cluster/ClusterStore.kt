package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import com.alphatalk.contracts.envelope.StreamCategory
import java.time.Instant

enum class ClusterStatus { NEW, SUMMARIZING, SUMMARIZED, IRRELEVANT }

data class ClusterRecord(
    val id: String,
    val repTitle: String,
    val summary: String?,
    val scope: String?,
    val status: ClusterStatus,
    val articleCount: Int,
    val category: StreamCategory = StreamCategory.NEWS,
)

data class ArticleRecord(
    val source: String,
    val sourceId: String,
    val url: String,
    val title: String,
    val excerpt: String?,
    val titleHash: String,
    val publishedAt: Instant,
    val fetchedAt: Instant,
)

data class StockLink(
    val code: String,
    val sentiment: String?,
    val confidence: Double?,
    val streamEventId: String?,
    val rejected: Boolean?,
)

data class DigestClusterRow(
    val clusterId: String,
    val title: String,
    val summary: String,
    val sentiment: String?,
    val articleCount: Int,
    val streamEventId: String?,
)

interface ClusterStore {
    fun findArticleCluster(sourceId: String): ClusterRecord?
    fun findClusterByTitleHash(titleHash: String, since: Instant, codes: List<String>): String?
    fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>?
    fun createCluster(
        id: String,
        repTitle: String,
        publishedAt: Instant,
        category: StreamCategory = StreamCategory.NEWS,
    )
    fun discardEmptyCluster(clusterId: String): Boolean
    fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?): Boolean
    fun addCandidateCode(clusterId: String, code: String)
    fun cluster(clusterId: String): ClusterRecord
    fun claimSummarize(clusterId: String, token: String, claimedAt: Instant, staleBefore: Instant): Boolean
    fun renewSummarize(clusterId: String, token: String, renewedAt: Instant): Boolean
    fun markSummarized(clusterId: String, token: String, summary: String, scope: String): Boolean
    fun markIrrelevant(clusterId: String, token: String): Boolean
    fun stockLinks(clusterId: String): List<StockLink>
    fun applyStockVerdict(clusterId: String, code: String, sentiment: String?, confidence: Double?, rejected: Boolean)
    fun claimStockEvent(clusterId: String, code: String, eventId: String): Boolean
    fun upsertSectorLink(clusterId: String, sectorCode: String, sentiment: String, confidence: Double, impact: String)
    fun articleSources(clusterId: String): List<SourceRef>
    fun representativeUrl(clusterId: String): String?
    fun stockClustersInWindow(code: String, from: Instant, to: Instant): List<DigestClusterRow>
    fun sectorClustersInWindow(sectorCode: String, stockCode: String, from: Instant, to: Instant): List<DigestClusterRow>
    fun marketClustersInWindow(from: Instant, to: Instant): List<DigestClusterRow>
}
