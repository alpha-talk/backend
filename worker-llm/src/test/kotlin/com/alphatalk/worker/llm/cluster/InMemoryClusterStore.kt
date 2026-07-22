package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import java.time.Instant
import kotlin.math.sqrt

class InMemoryClusterStore : ClusterStore {
    data class ClusterState(
        var repTitle: String,
        var summary: String? = null,
        var scope: String? = null,
        var status: ClusterStatus = ClusterStatus.NEW,
        var firstAt: Instant,
        var lastAt: Instant,
        var articleCount: Int = 0,
    )

    val clusters = linkedMapOf<String, ClusterState>()
    val articles = linkedMapOf<String, Pair<ArticleRecord, String>>()
    val articleEmbeddings = linkedMapOf<String, FloatArray>()
    val stockLinkRows = linkedMapOf<Pair<String, String>, StockLink>()
    val sectorLinkRows = linkedMapOf<Pair<String, String>, Triple<String, Double, String>>()

    override fun findArticleCluster(sourceId: String): ClusterRecord? =
        articles[sourceId]?.let { (_, clusterId) -> cluster(clusterId) }

    override fun findClusterByTitleHash(titleHash: String, since: Instant): String? =
        articles.values
            .filter { (a, cid) -> a.titleHash == titleHash && clusters.getValue(cid).lastAt >= since }
            .maxByOrNull { (_, cid) -> clusters.getValue(cid).lastAt }
            ?.second

    override fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>? =
        articles.values.mapNotNull { (a, cid) ->
            val other = articleEmbeddings[a.sourceId] ?: return@mapNotNull null
            val state = clusters.getValue(cid)
            if (state.lastAt < since) return@mapNotNull null
            val codeOverlap = state.status == ClusterStatus.NEW ||
                stockLinkRows.keys.any { it.first == cid && it.second in codes }
            if (codes.isNotEmpty() && !codeOverlap) return@mapNotNull null
            cid to cosine(embedding, other)
        }.maxByOrNull { it.second }

    override fun createCluster(id: String, repTitle: String, publishedAt: Instant) {
        clusters[id] = ClusterState(repTitle = repTitle, firstAt = publishedAt, lastAt = publishedAt)
    }

    override fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?) {
        if (articles.containsKey(article.sourceId)) return
        articles[article.sourceId] = article to clusterId
        embedding?.let { articleEmbeddings[article.sourceId] = it }
        clusters.getValue(clusterId).let {
            it.articleCount++
            if (article.publishedAt > it.lastAt) it.lastAt = article.publishedAt
        }
    }

    override fun cluster(clusterId: String): ClusterRecord = clusters.getValue(clusterId).let {
        ClusterRecord(clusterId, it.repTitle, it.summary, it.scope, it.status, it.articleCount)
    }

    override fun markSummarized(clusterId: String, summary: String, scope: String) {
        clusters.getValue(clusterId).let {
            it.status = ClusterStatus.SUMMARIZED
            it.summary = summary
            it.scope = scope
        }
    }

    override fun markIrrelevant(clusterId: String) {
        clusters.getValue(clusterId).status = ClusterStatus.IRRELEVANT
    }

    override fun stockLinks(clusterId: String): List<StockLink> =
        stockLinkRows.filterKeys { it.first == clusterId }.values.toList()

    override fun insertStockLinkIfAbsent(clusterId: String, code: String, sentiment: String?, confidence: Double?): Boolean {
        val key = clusterId to code
        if (stockLinkRows.containsKey(key)) return false
        stockLinkRows[key] = StockLink(code, sentiment, confidence, null)
        return true
    }

    override fun setStockLinkEvent(clusterId: String, code: String, streamEventId: String) {
        val key = clusterId to code
        stockLinkRows[key] = stockLinkRows.getValue(key).copy(streamEventId = streamEventId)
    }

    override fun upsertSectorLink(clusterId: String, sectorCode: String, sentiment: String, confidence: Double, impact: String) {
        sectorLinkRows[clusterId to sectorCode] = Triple(sentiment, confidence, impact)
    }

    override fun articleSources(clusterId: String): List<SourceRef> =
        articles.values.filter { it.second == clusterId }.map { SourceRef(it.first.source, it.first.url) }

    override fun representativeUrl(clusterId: String): String? =
        articles.values.firstOrNull { it.second == clusterId }?.first?.url

    override fun stockClustersInWindow(code: String, from: Instant, to: Instant): List<DigestClusterRow> =
        stockLinkRows.filterKeys { it.second == code }.entries.mapNotNull { (key, link) ->
            digestRow(key.first, from, to, link.sentiment, link.streamEventId)
        }

    override fun sectorClustersInWindow(sectorCode: String, from: Instant, to: Instant): List<DigestClusterRow> =
        sectorLinkRows.filterKeys { it.second == sectorCode }.entries.mapNotNull { (key, value) ->
            digestRow(key.first, from, to, value.first, null)
        }

    override fun marketClustersInWindow(from: Instant, to: Instant): List<DigestClusterRow> =
        clusters.entries.filter { it.value.scope == "MARKET" }.mapNotNull { (id, _) ->
            digestRow(id, from, to, null, null)
        }

    private fun digestRow(clusterId: String, from: Instant, to: Instant, sentiment: String?, eventId: String?): DigestClusterRow? {
        val state = clusters.getValue(clusterId)
        if (state.status != ClusterStatus.SUMMARIZED) return null
        if (state.lastAt < from || state.lastAt >= to) return null
        return DigestClusterRow(clusterId, state.repTitle, state.summary.orEmpty(), sentiment, state.articleCount, eventId)
    }

    private fun cosine(a: FloatArray, b: FloatArray): Double {
        var dot = 0.0
        var na = 0.0
        var nb = 0.0
        for (i in a.indices) {
            dot += a[i] * b[i]
            na += a[i] * a[i]
            nb += b[i] * b[i]
        }
        if (na == 0.0 || nb == 0.0) return 0.0
        return dot / (sqrt(na) * sqrt(nb))
    }
}

class NoopClusterLock : ClusterLock {
    override fun <T> withLock(code: String, action: () -> T): T = action()
}
