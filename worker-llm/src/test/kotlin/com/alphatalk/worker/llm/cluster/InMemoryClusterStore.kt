package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.envelope.SourceRef
import com.alphatalk.contracts.envelope.StreamCategory
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
        var summarizingAt: Instant? = null,
        var summarizingToken: String? = null,
        var category: StreamCategory = StreamCategory.NEWS,
    )

    val clusters = linkedMapOf<String, ClusterState>()
    val articles = linkedMapOf<String, Pair<ArticleRecord, String>>()
    val articleEmbeddings = linkedMapOf<String, FloatArray>()
    val stockLinkRows = linkedMapOf<Pair<String, String>, StockLink>()
    val sectorLinkRows = linkedMapOf<Pair<String, String>, Triple<String, Double, String>>()

    @Synchronized
    override fun findArticleCluster(sourceId: String): ClusterRecord? =
        articles[sourceId]?.let { (_, clusterId) -> cluster(clusterId) }

    @Synchronized
    override fun findClusterByTitleHash(titleHash: String, since: Instant, codes: List<String>): String? =
        articles.values
            .filter { (a, cid) ->
                a.titleHash == titleHash && clusters.getValue(cid).lastAt >= since && matchesCandidateBoundary(cid, codes)
            }
            .maxByOrNull { (_, cid) -> clusters.getValue(cid).lastAt }
            ?.second

    @Synchronized
    override fun nearestCluster(embedding: FloatArray, since: Instant, codes: List<String>): Pair<String, Double>? =
        articles.values.mapNotNull { (a, cid) ->
            val other = articleEmbeddings[a.sourceId] ?: return@mapNotNull null
            if (clusters.getValue(cid).lastAt < since) return@mapNotNull null
            if (!matchesCandidateBoundary(cid, codes)) return@mapNotNull null
            cid to cosine(embedding, other)
        }.maxByOrNull { it.second }

    private fun matchesCandidateBoundary(clusterId: String, codes: List<String>): Boolean {
        val state = clusters.getValue(clusterId)
        if (state.status == ClusterStatus.IRRELEVANT) return false
        if (codes.isNotEmpty()) return stockLinkRows.keys.any { it.first == clusterId && it.second in codes }
        return state.scope == "SECTOR" || state.scope == "MARKET" ||
            articles.values.any { (article, candidateClusterId) ->
                candidateClusterId == clusterId && article.candidateCodesEmpty
            } ||
            (state.status in setOf(ClusterStatus.NEW, ClusterStatus.SUMMARIZING) &&
                stockLinkRows.keys.none { it.first == clusterId })
    }

    @Synchronized
    override fun createCluster(id: String, repTitle: String, publishedAt: Instant, category: StreamCategory) {
        clusters[id] = ClusterState(repTitle = repTitle, firstAt = publishedAt, lastAt = publishedAt, category = category)
    }

    @Synchronized
    override fun discardEmptyCluster(clusterId: String): Boolean {
        val state = clusters[clusterId] ?: return false
        if (state.status != ClusterStatus.NEW || state.articleCount != 0) return false
        if (articles.values.any { it.second == clusterId }) return false
        if (stockLinkRows.keys.any { it.first == clusterId }) return false
        if (sectorLinkRows.keys.any { it.first == clusterId }) return false
        clusters.remove(clusterId)
        return true
    }

    @Synchronized
    override fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?): Boolean {
        if (articles.containsKey(article.sourceId)) return false
        articles[article.sourceId] = article to clusterId
        embedding?.let { articleEmbeddings[article.sourceId] = it }
        clusters.getValue(clusterId).let {
            it.articleCount++
            if (article.publishedAt > it.lastAt) it.lastAt = article.publishedAt
        }
        return true
    }

    @Synchronized
    override fun addCandidateCode(clusterId: String, code: String) {
        stockLinkRows.putIfAbsent(clusterId to code, StockLink(code, null, null, null, null))
    }

    @Synchronized
    override fun cluster(clusterId: String): ClusterRecord = clusters.getValue(clusterId).let {
        ClusterRecord(clusterId, it.repTitle, it.summary, it.scope, it.status, it.articleCount, it.category)
    }

    @Synchronized
    override fun claimSummarize(clusterId: String, token: String, claimedAt: Instant, staleBefore: Instant): Boolean {
        val state = clusters.getValue(clusterId)
        val claimable = state.status == ClusterStatus.NEW ||
            (state.status == ClusterStatus.SUMMARIZING && (state.summarizingAt?.isBefore(staleBefore) ?: true))
        if (!claimable) return false
        state.status = ClusterStatus.SUMMARIZING
        state.summarizingAt = claimedAt
        state.summarizingToken = token
        return true
    }

    @Synchronized
    override fun renewSummarize(clusterId: String, token: String, renewedAt: Instant): Boolean {
        val state = clusters.getValue(clusterId)
        if (state.status != ClusterStatus.SUMMARIZING || state.summarizingToken != token) return false
        state.summarizingAt = renewedAt
        return true
    }

    @Synchronized
    override fun markSummarized(clusterId: String, token: String, summary: String, scope: String): Boolean {
        val state = clusters.getValue(clusterId)
        if (state.status != ClusterStatus.SUMMARIZING || state.summarizingToken != token) return false
        state.let {
            it.status = ClusterStatus.SUMMARIZED
            it.summary = summary
            it.scope = scope
            it.summarizingAt = null
            it.summarizingToken = null
        }
        return true
    }

    @Synchronized
    override fun markIrrelevant(clusterId: String, token: String): Boolean {
        val state = clusters.getValue(clusterId)
        if (state.status != ClusterStatus.SUMMARIZING || state.summarizingToken != token) return false
        state.status = ClusterStatus.IRRELEVANT
        state.summarizingAt = null
        state.summarizingToken = null
        return true
    }

    @Synchronized
    override fun stockLinks(clusterId: String): List<StockLink> =
        stockLinkRows.filterKeys { it.first == clusterId }.values.toList()

    @Synchronized
    override fun applyStockVerdict(
        clusterId: String,
        code: String,
        sentiment: String?,
        confidence: Double?,
        rejected: Boolean,
    ) {
        val key = clusterId to code
        val existing = stockLinkRows[key]
        stockLinkRows[key] = StockLink(code, sentiment, confidence, existing?.streamEventId, rejected)
    }

    @Synchronized
    override fun claimStockEvent(clusterId: String, code: String, eventId: String): Boolean {
        val key = clusterId to code
        val link = stockLinkRows[key] ?: return false
        if (link.streamEventId != null) return false
        stockLinkRows[key] = link.copy(streamEventId = eventId)
        return true
    }

    @Synchronized
    override fun upsertSectorLink(clusterId: String, sectorCode: String, sentiment: String, confidence: Double, impact: String) {
        sectorLinkRows[clusterId to sectorCode] = Triple(sentiment, confidence, impact)
    }

    @Synchronized
    override fun articleSources(clusterId: String): List<SourceRef> =
        articles.values.filter { it.second == clusterId }.map { SourceRef(it.first.source, it.first.url) }

    @Synchronized
    override fun representativeUrl(clusterId: String): String? =
        articles.values.firstOrNull { it.second == clusterId }?.first?.url

    @Synchronized
    override fun stockClustersInWindow(code: String, from: Instant, to: Instant): List<DigestClusterRow> =
        stockLinkRows.filterKeys { it.second == code }.entries.mapNotNull { (key, link) ->
            if (link.streamEventId == null) return@mapNotNull null
            if (clusters.getValue(key.first).scope != "STOCK") return@mapNotNull null
            digestRow(key.first, from, to, link.sentiment, link.streamEventId)
        }

    @Synchronized
    override fun sectorClustersInWindow(
        sectorCode: String,
        stockCode: String,
        from: Instant,
        to: Instant,
    ): List<DigestClusterRow> =
        sectorLinkRows.filterKeys { it.second == sectorCode }.entries.mapNotNull { (key, value) ->
            if (clusters.getValue(key.first).scope != "SECTOR") return@mapNotNull null
            digestRow(key.first, from, to, value.first, stockLinkRows[key.first to stockCode]?.streamEventId)
        }

    @Synchronized
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
