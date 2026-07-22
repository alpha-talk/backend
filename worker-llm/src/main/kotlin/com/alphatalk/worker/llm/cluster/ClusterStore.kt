package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import java.time.Instant

enum class ClusterStatus { NEW, SUMMARIZED, IRRELEVANT }

data class ClusterRecord(
    val id: String,
    val repTitle: String,
    val summary: String?,
    val scope: String?,
    val status: ClusterStatus,
    val articleCount: Int,
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
    fun findClusterByTitleHash(titleHash: String, since: Instant): String?
    fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>?
    fun createCluster(id: String, repTitle: String, publishedAt: Instant)
    fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?)
    fun cluster(clusterId: String): ClusterRecord
    fun markSummarized(clusterId: String, summary: String, scope: String)
    fun markIrrelevant(clusterId: String)
    fun stockLinks(clusterId: String): List<StockLink>
    fun insertStockLinkIfAbsent(clusterId: String, code: String, sentiment: String?, confidence: Double?): Boolean
    fun setStockLinkEvent(clusterId: String, code: String, streamEventId: String)
    fun upsertSectorLink(clusterId: String, sectorCode: String, sentiment: String, confidence: Double, impact: String)
    fun articleSources(clusterId: String): List<SourceRef>
    fun representativeUrl(clusterId: String): String?
    fun stockClustersInWindow(code: String, from: Instant, to: Instant): List<DigestClusterRow>
    fun sectorClustersInWindow(sectorCode: String, from: Instant, to: Instant): List<DigestClusterRow>
    fun marketClustersInWindow(from: Instant, to: Instant): List<DigestClusterRow>
}
