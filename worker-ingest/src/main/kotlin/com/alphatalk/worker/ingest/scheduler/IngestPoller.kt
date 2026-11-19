package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestConfig
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.inbox.IngestInbox
import com.alphatalk.worker.ingest.normalize.ArticleNormalizer
import com.alphatalk.worker.ingest.source.FeedValidators
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.dao.ConcurrencyFailureException
import org.springframework.dao.DataAccessException
import org.springframework.stereotype.Service
import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import java.util.concurrent.ThreadLocalRandom

@Service
class IngestPoller(
    private val sources: List<NewsSource>,
    private val inbox: IngestInbox,
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
        return runCatching {
            val checkpoint = inbox.checkpoint(source.id, source.url)
            val update = source.fetchLatest(checkpoint?.validators ?: FeedValidators()) ?: return PollStats()
            val entries = update.articles.map { toEntry(source.name, it) }
            val saved = captureWithRetry {
                inbox.capture(source.id, source.url, checkpoint, update.validators, entries)
            }
            PollStats(fetched = entries.size, captured = saved, duplicateSkipped = entries.size - saved)
        }.getOrElse {
            log.warn("source poll failed: id={} source={}", source.id, source.name, it)
            PollStats(sourceErrors = 1)
        }
    }

    private fun captureWithRetry(capture: () -> Int): Int {
        repeat(2) { attempt ->
            try {
                return capture()
            } catch (failure: DataAccessException) {
                val sqlState = (failure.mostSpecificCause as? SQLException)?.sqlState
                if (failure !is ConcurrencyFailureException && sqlState != UNIQUE_VIOLATION_SQL_STATE) throw failure
                val delay = 10L shl attempt
                Thread.sleep(ThreadLocalRandom.current().nextLong(delay, delay * 2))
            }
        }
        return capture()
    }

    private fun toEntry(sourceName: String, article: FetchedArticle): IngestQueueEntry {
        val url = ArticleNormalizer.normalizeUrl(article.url)
        return IngestQueueEntry(
            source = sourceName,
            sourceId = ArticleNormalizer.sourceId(sourceName, article.sourceId, url),
            type = IngestType.NEWS,
            codes = article.codes.distinct().sorted(),
            title = article.title,
            url = url,
            body = article.excerpt?.take(props.excerptMaxLength),
            fetchedAt = article.publishedAt ?: clock(),
        )
    }

    private companion object {
        const val UNIQUE_VIOLATION_SQL_STATE = "23505"
    }
}

data class PollStats(
    val fetched: Int = 0,
    val captured: Int = 0,
    val duplicateSkipped: Int = 0,
    val sourceErrors: Int = 0,
) {
    operator fun plus(other: PollStats) = PollStats(
        fetched = fetched + other.fetched,
        captured = captured + other.captured,
        duplicateSkipped = duplicateSkipped + other.duplicateSkipped,
        sourceErrors = sourceErrors + other.sourceErrors,
    )
}
