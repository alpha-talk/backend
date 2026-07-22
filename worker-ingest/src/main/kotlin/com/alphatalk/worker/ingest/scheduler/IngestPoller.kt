package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.mapping.StockCodeMapper
import com.alphatalk.worker.ingest.normalize.ArticleNormalizer
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

class IngestPoller(
    private val sources: List<NewsSource>,
    private val mapper: StockCodeMapper,
    private val seen: SeenMarker,
    private val queue: IngestQueue,
    private val excerptMaxLength: Int,
    private val fetchExecutor: Executor = Executor { it.run() },
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
            log.warn("source poll failed: source={}", source.name, it)
            stats.sourceErrors++
        }
        return stats.snapshot()
    }

    private fun process(sourceName: String, article: FetchedArticle, stats: Counters) {
        val mapping = mapper.map(article.title, article.excerpt)
        if (mapping.unmatched) {
            stats.unmatchedSkipped++
            return
        }
        val url = ArticleNormalizer.normalizeUrl(article.url)
        val sourceId = ArticleNormalizer.sourceId(sourceName, article.sourceId, url)
        if (!seen.markIfNew(sourceId)) {
            stats.duplicateSkipped++
            return
        }
        val entry = IngestQueueEntry(
            source = sourceName,
            sourceId = sourceId,
            type = IngestType.NEWS,
            codes = mapping.codes,
            title = article.title,
            url = url,
            body = article.excerpt?.take(excerptMaxLength),
            fetchedAt = article.publishedAt ?: clock(),
            macroHint = mapping.macroHint,
        )
        runCatching { queue.enqueue(entry) }
            .onSuccess { stats.enqueued++ }
            .onFailure {
                log.warn("enqueue failed: sourceId={}", sourceId, it)
                seen.clear(sourceId)
                stats.enqueueErrors++
            }
    }

    private class Counters(
        var fetched: Int = 0,
        var enqueued: Int = 0,
        var duplicateSkipped: Int = 0,
        var unmatchedSkipped: Int = 0,
        var sourceErrors: Int = 0,
        var enqueueErrors: Int = 0,
    ) {
        fun snapshot() = PollStats(fetched, enqueued, duplicateSkipped, unmatchedSkipped, sourceErrors, enqueueErrors)
    }
}

data class PollStats(
    val fetched: Int = 0,
    val enqueued: Int = 0,
    val duplicateSkipped: Int = 0,
    val unmatchedSkipped: Int = 0,
    val sourceErrors: Int = 0,
    val enqueueErrors: Int = 0,
) {
    operator fun plus(other: PollStats) = PollStats(
        fetched = fetched + other.fetched,
        enqueued = enqueued + other.enqueued,
        duplicateSkipped = duplicateSkipped + other.duplicateSkipped,
        unmatchedSkipped = unmatchedSkipped + other.unmatchedSkipped,
        sourceErrors = sourceErrors + other.sourceErrors,
        enqueueErrors = enqueueErrors + other.enqueueErrors,
    )
}
