package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.queue.IngestQueueEntry
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.time.Instant

class ClusterAssigner(
    private val store: ClusterStore,
    private val embeddings: EmbeddingClient,
    private val lock: ClusterLock,
    private val window: Duration,
    private val similarityThreshold: Double,
    private val clusterIds: () -> String,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun assign(entry: IngestQueueEntry): AssignResult {
        store.findArticleCluster(entry.sourceId)?.let {
            entry.codes.forEach { code -> store.addCandidateCode(it.id, code) }
            return AssignResult(it.id, joined = false, created = false)
        }
        val titleHash = TitleNormalizer.hash(entry.title)
        val since = clock.instant().minus(window)
        val article = articleOf(entry, titleHash)
        val embedding = if (store.findClusterByTitleHash(titleHash, since, entry.codes) == null) {
            runCatching { embeddings.embed(embedText(entry)) }
                .onFailure { log.warn("embedding failed, falling back to new cluster: sourceId={}", entry.sourceId, it) }
                .getOrNull()
        } else {
            null
        }

        val lockKey = entry.codes.firstOrNull() ?: MACRO_LOCK_KEY
        return lock.withLock(lockKey) {
            store.findArticleCluster(entry.sourceId)?.let {
                entry.codes.forEach { code -> store.addCandidateCode(it.id, code) }
                return@withLock AssignResult(it.id, joined = false, created = false)
            }
            store.findClusterByTitleHash(titleHash, since, entry.codes)?.let { clusterId ->
                val landedClusterId = land(article, clusterId, null, entry.codes)
                return@withLock AssignResult(landedClusterId, joined = true, created = false)
            }
            val nearest = embedding?.let { store.nearestCluster(it, since, entry.codes) }
            if (nearest != null && nearest.second >= similarityThreshold) {
                val landedClusterId = land(article, nearest.first, embedding, entry.codes)
                return@withLock AssignResult(landedClusterId, joined = true, created = false)
            }
            val clusterId = clusterIds()
            store.createCluster(clusterId, entry.title, article.publishedAt)
            val landedClusterId = land(article, clusterId, embedding, entry.codes)
            if (landedClusterId != clusterId) store.discardEmptyCluster(clusterId)
            AssignResult(
                clusterId = landedClusterId,
                joined = landedClusterId != clusterId,
                created = landedClusterId == clusterId,
            )
        }
    }

    private fun land(article: ArticleRecord, clusterId: String, embedding: FloatArray?, codes: List<String>): String {
        val landedClusterId = if (store.attachArticle(article, clusterId, embedding)) {
            clusterId
        } else {
            store.findArticleCluster(article.sourceId)?.id
                ?: throw IllegalStateException("article attach lost without existing cluster: ${article.sourceId}")
        }
        codes.forEach { store.addCandidateCode(landedClusterId, it) }
        return landedClusterId
    }

    private fun articleOf(entry: IngestQueueEntry, titleHash: String): ArticleRecord {
        val now = clock.instant()
        return ArticleRecord(
            source = entry.source,
            sourceId = entry.sourceId,
            url = entry.url,
            title = entry.title,
            excerpt = entry.body,
            titleHash = titleHash,
            publishedAt = Instant.ofEpochMilli(entry.fetchedAt),
            fetchedAt = now,
        )
    }

    private fun embedText(entry: IngestQueueEntry): String =
        listOfNotNull(TitleNormalizer.normalize(entry.title), entry.body?.take(300)).joinToString(" ")

    companion object {
        private const val MACRO_LOCK_KEY = "macro"
    }
}

data class AssignResult(
    val clusterId: String,
    val joined: Boolean,
    val created: Boolean,
)
