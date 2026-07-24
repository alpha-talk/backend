package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.IngestQueue
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId

@Component
@ConditionalOnProperty("alphatalk.ingest.digest.enabled", havingValue = "true", matchIfMissing = true)
class DigestTrigger(
    private val queue: IngestQueue,
    private val props: IngestProperties,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.ingest.digest.cron:0 0 18 * * *}", zone = "\${alphatalk.ingest.digest.zone:Asia/Seoul}")
    fun trigger() {
        triggerFor(LocalDate.now(clock.withZone(ZoneId.of(props.digest.zone))))
    }

    fun triggerFor(date: LocalDate): Int {
        var enqueued = 0
        props.stocks.forEach { stock ->
            runCatching {
                queue.enqueue(
                    IngestQueueEntry(
                        source = IngestQueueEntry.DIGEST_SOURCE,
                        sourceId = IngestQueueEntry.digestSourceId(stock.code, date.toString()),
                        type = IngestType.DIGEST,
                        codes = listOf(stock.code),
                        title = "",
                        url = "",
                        fetchedAt = clock.millis(),
                    ),
                )
                enqueued++
            }.onFailure {
                log.warn("digest job enqueue failed: code={}", stock.code, it)
            }
        }
        log.info("digest jobs enqueued: {}/{} date={}", enqueued, props.stocks.size, date)
        return enqueued
    }
}
