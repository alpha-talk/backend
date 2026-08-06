package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestConfig
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.queue.IngestQueue
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.Executor

@Component
@ConditionalOnProperty("alphatalk.ingest.digest.enabled", havingValue = "true", matchIfMissing = true)
class DigestTrigger(
    private val queue: IngestQueue,
    private val seen: SeenMarker,
    private val props: IngestProperties,
    @Qualifier(IngestConfig.CATCH_UP_EXECUTOR_BEAN)
    private val catchUpExecutor: Executor,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val zone = ZoneId.of(props.digest.zone)
    private val cron = props.digest.cron
        .takeIf { it != Scheduled.CRON_DISABLED }
        ?.let(CronExpression::parse)

    @Scheduled(cron = "\${alphatalk.ingest.digest.cron:0 0 18 * * *}", zone = "\${alphatalk.ingest.digest.zone:Asia/Seoul}")
    fun trigger() {
        triggerFor(LocalDate.now(clock.withZone(zone)))
    }

    @EventListener(ApplicationReadyEvent::class)
    fun catchUpOnStartup() {
        if (!props.digest.catchUpOnStartup) return
        catchUpExecutor.execute(::catchUp)
    }

    private fun catchUp() {
        val now = ZonedDateTime.now(clock.withZone(zone))
        val missed = lastFireAtOrBefore(now)
        if (missed == null) {
            log.info("digest catch-up skipped: no elapsed schedule now={}", now)
            return
        }
        log.info("digest catch-up start: firedAt={} now={}", missed, now)
        runCatching { triggerFor(missed.toLocalDate()) }
            .onFailure { log.warn("digest catch-up failed: firedAt={}", missed, it) }
    }

    fun triggerFor(date: LocalDate): Int {
        var enqueued = 0
        var skipped = 0
        props.stocks.forEach { stock ->
            val sourceId = IngestQueueEntry.digestSourceId(stock.code, date.toString())
            runCatching {
                if (!seen.markIfNew(sourceId)) {
                    skipped++
                    return@runCatching
                }
                runCatching { queue.enqueue(entryOf(stock.code, sourceId)) }
                    .onFailure {
                        seen.clear(sourceId)
                        throw it
                    }
                enqueued++
            }.onFailure {
                log.warn("digest job enqueue failed: code={}", stock.code, it)
            }
        }
        log.info(
            "digest jobs enqueued: {}/{} skipped={} date={}",
            enqueued, props.stocks.size, skipped, date,
        )
        return enqueued
    }

    private fun entryOf(code: String, sourceId: String) =
        IngestQueueEntry(
            source = IngestQueueEntry.DIGEST_SOURCE,
            sourceId = sourceId,
            type = IngestType.DIGEST,
            codes = listOf(code),
            title = "",
            url = "",
            fetchedAt = clock.millis(),
        )

    private fun lastFireAtOrBefore(now: ZonedDateTime): ZonedDateTime? {
        val expression = cron ?: return null
        for (daysBack in 0..CATCH_UP_LOOKBACK_DAYS) {
            val date = now.toLocalDate().minusDays(daysBack.toLong())
            var fire = expression.next(date.atStartOfDay(zone).minusNanos(1))
            var last: ZonedDateTime? = null
            while (fire != null && fire.toLocalDate() == date && !fire.isAfter(now)) {
                last = fire
                fire = expression.next(fire)
            }
            if (last != null) return last
        }
        return null
    }

    private companion object {
        const val CATCH_UP_LOOKBACK_DAYS = 7
    }
}
