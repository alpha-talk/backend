package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestConfig
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.normalize.ArticleNormalizer
import com.alphatalk.worker.ingest.queue.EnqueueResult
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

@Service
class IngestPoller(
    private val sources: List<NewsSource>,
    private val queue: IngestQueue,
    private val props: IngestProperties,
    @Qualifier(IngestConfig.FETCH_EXECUTOR_BEAN)
    private val fetchExecutor: Executor,
    private val clock: () -> Long = System::currentTimeMillis,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun pollOnce(): PollStats =
        sources
            .map { source -> CompletableFuture.supplyAsync({ pollSource(source) }, fetchExecutor) }
            .map { it.join() }
            .fold(PollStats()) { merged, stats -> merged + stats }

    private fun pollSource(source: NewsSource): PollStats {
        val stats = Counters()
        runCatching {
            for (article in source.fetchLatest()) {
                stats.fetched++
                process(source.name, article, stats)
            }
        }.onFailure {
            log.warn("source poll failed: id={} source={}", source.id, source.name, it)
            stats.sourceErrors++
        }
        return stats.snapshot()
    }

    private fun process(sourceName: String, article: FetchedArticle, stats: Counters) {
        val codes = article.codes.distinct().sorted()
        val url = ArticleNormalizer.normalizeUrl(article.url)
        val sourceId = ArticleNormalizer.sourceId(sourceName, article.sourceId, url)
        val entry = IngestQueueEntry(
            source = sourceName,
            sourceId = sourceId,
            type = IngestType.NEWS,
            codes = codes,
            title = article.title,
            url = url,
            body = article.excerpt?.take(props.excerptMaxLength),
            fetchedAt = article.publishedAt ?: clock(),
        )
        runCatching { queue.enqueueIfNew(entry) }
            .onSuccess { result ->
                when (result) {
                    EnqueueResult.ENQUEUED -> stats.enqueued++
                    EnqueueResult.ALREADY_ENQUEUED -> stats.duplicateSkipped++
                }
            }
            .onFailure {
                log.warn("enqueue failed: sourceId={}", sourceId, it)
                stats.enqueueErrors++
            }
    }

    private class Counters(
        var fetched: Int = 0,
        var enqueued: Int = 0,
        var duplicateSkipped: Int = 0,
        var sourceErrors: Int = 0,
        var enqueueErrors: Int = 0,
    ) {
        fun snapshot() = PollStats(fetched, enqueued, duplicateSkipped, sourceErrors, enqueueErrors)
    }
}

data class PollStats(
    val fetched: Int = 0,
    val enqueued: Int = 0,
    val duplicateSkipped: Int = 0,
    val sourceErrors: Int = 0,
    val enqueueErrors: Int = 0,
) {
    operator fun plus(other: PollStats) = PollStats(
        fetched = fetched + other.fetched,
        enqueued = enqueued + other.enqueued,
        duplicateSkipped = duplicateSkipped + other.duplicateSkipped,
        sourceErrors = sourceErrors + other.sourceErrors,
        enqueueErrors = enqueueErrors + other.enqueueErrors,
    )
}
