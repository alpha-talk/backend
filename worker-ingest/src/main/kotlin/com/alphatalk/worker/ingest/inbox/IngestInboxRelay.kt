package com.alphatalk.worker.ingest.inbox

import com.alphatalk.worker.ingest.queue.EnqueueResult
import com.alphatalk.worker.ingest.queue.RedisIngestQueue
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.data.domain.PageRequest
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant

@Component
@ConditionalOnProperty("alphatalk.ingest.poll-enabled", havingValue = "true", matchIfMissing = true)
class IngestInboxRelay(
    private val articles: IngestInboxRepository,
    private val inbox: JpaIngestInbox,
    private val queue: RedisIngestQueue,
    meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val enqueued = meters.counter("ingest.enqueued")
    private val errors = meters.counter("ingest.enqueue.errors")

    @Scheduled(fixedDelayString = "\${alphatalk.ingest.relay-delay:5s}")
    fun relay() {
        val now = Instant.now()
        val retryAt = now.plus(Duration.ofMinutes(1))
        val batch = articles.findReady(now, PageRequest.of(0, 100))
        for (row in batch) {
            if (articles.deferIfReady(row.sourceId, now, retryAt) == 0) continue
            val result = runCatching { queue.enqueueCaptured(inbox.decode(row)) }.getOrElse {
                errors.increment()
                log.warn("captured article enqueue failed: sourceId={}", row.sourceId, it)
                return
            }
            if (result == EnqueueResult.ENQUEUED) enqueued.increment()
        }
    }
}
