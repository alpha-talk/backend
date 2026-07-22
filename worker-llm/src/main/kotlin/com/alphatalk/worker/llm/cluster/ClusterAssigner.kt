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
        val lockKey = entry.codes.firstOrNull() ?: MACRO_LOCK_KEY
        return lock.withLock(lockKey) {
            store.findArticleCluster(entry.sourceId)?.let {
                return@withLock AssignResult(it.id, joined = false, created = false)
            }
            val titleHash = TitleNormalizer.hash(entry.title)
            val since = clock.instant().minus(window)
            val article = articleOf(entry, titleHash)

            store.findClusterByTitleHash(titleHash, since)?.let { clusterId ->
                store.attachArticle(article, clusterId, null)
                return@withLock AssignResult(clusterId, joined = true, created = false)
            }

            val embedding = runCatching { embeddings.embed(embedText(entry)) }
                .onFailure { log.warn("embedding failed, falling back to new cluster: sourceId={}", entry.sourceId, it) }
                .getOrNull()
            val nearest = embedding?.let { store.nearestCluster(it, since, entry.codes) }
            if (nearest != null && nearest.second >= similarityThreshold) {
                store.attachArticle(article, nearest.first, embedding)
                return@withLock AssignResult(nearest.first, joined = true, created = false)
            }

            val clusterId = clusterIds()
            store.createCluster(clusterId, entry.title, article.publishedAt)
            store.attachArticle(article, clusterId, embedding)
            AssignResult(clusterId, joined = false, created = true)
        }
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
