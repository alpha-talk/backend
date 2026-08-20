package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestConfig
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.EnqueueResult
import com.alphatalk.worker.ingest.queue.IngestQueue
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.LocalDate
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor

@Component
@ConditionalOnProperty("alphatalk.ingest.digest.enabled", havingValue = "true", matchIfMissing = true)
class DigestTrigger(
    private val queue: IngestQueue,
    private val universe: DigestUniverse,
    private val props: IngestProperties,
    private val meters: MeterRegistry,
    @param:Qualifier(IngestConfig.CATCH_UP_EXECUTOR_BEAN)
    catchUpExecutor: Executor,
    @param:Qualifier(IngestConfig.DIGEST_ENQUEUE_EXECUTOR_BEAN)
    private val enqueueExecutor: Executor,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val catchUp = DigestCatchUp(
        cronExpression = props.digest.cron,
        zoneId = props.digest.zone,
        clock = clock,
        executor = catchUpExecutor,
        meters = meters,
        metricPrefix = "ingest.digest.catchup",
    ) { date ->
        val result = triggerForResult(date)
        record(result)
        result.complete
    }

    @Scheduled(cron = "\${alphatalk.ingest.digest.cron:0 0 18 * * *}", zone = "\${alphatalk.ingest.digest.zone:Asia/Seoul}")
    fun trigger() {
        val date = catchUp.fireDate()
        val result = triggerForResult(date)
        record(result)
        if (result.complete) catchUp.markReconciled(date)
    }

    @EventListener(ApplicationReadyEvent::class)
    fun catchUpOnStartup() {
        if (!props.digest.catchUpOnStartup) return
        catchUp.submit()
    }

    @Scheduled(
        fixedDelayString = "\${alphatalk.ingest.digest.catch-up-reconcile-delay:1m}",
        initialDelayString = "\${alphatalk.ingest.digest.catch-up-reconcile-delay:1m}",
    )
    fun reconcileCatchUp() {
        if (!props.digest.catchUpOnStartup) return
        catchUp.submit()
    }

    fun triggerFor(date: LocalDate): Int = triggerForResult(date).enqueued

    private fun triggerForResult(date: LocalDate): DigestTriggerResult {
        val codes = runCatching { universe.codes() }.getOrElse {
            log.warn("digest universe lookup failed - retried on next reconcile: date={}", date, it)
            return DigestTriggerResult(enqueued = 0, skipped = 0, errors = 1)
        }
        val outcomes = codes
            .map { code -> CompletableFuture.supplyAsync({ enqueueOne(code, date) }, enqueueExecutor) }
            .map { it.join() }
        val result = DigestTriggerResult(
            enqueued = outcomes.count { it == EnqueueOutcome.ENQUEUED },
            skipped = outcomes.count { it == EnqueueOutcome.SKIPPED },
            errors = outcomes.count { it == EnqueueOutcome.FAILED },
        )
        log.info(
            "digest jobs enqueued: {}/{} skipped={} errors={} date={}",
            result.enqueued, codes.size, result.skipped, result.errors, date,
        )
        return result
    }

    private fun enqueueOne(code: String, date: LocalDate): EnqueueOutcome {
        val sourceId = IngestQueueEntry.digestSourceId(code, date.toString())
        return runCatching { queue.enqueueIfNew(entryOf(code, sourceId)) }
            .map { result ->
                when (result) {
                    EnqueueResult.ENQUEUED -> EnqueueOutcome.ENQUEUED
                    EnqueueResult.ALREADY_ENQUEUED -> EnqueueOutcome.SKIPPED
                }
            }
            .getOrElse {
                log.warn("digest job enqueue failed: code={}", code, it)
                EnqueueOutcome.FAILED
            }
    }

    private enum class EnqueueOutcome { ENQUEUED, SKIPPED, FAILED }

    private fun record(result: DigestTriggerResult) {
        meters.counter("ingest.digest.enqueued").increment(result.enqueued.toDouble())
        meters.counter("ingest.digest.duplicate.skipped").increment(result.skipped.toDouble())
        meters.counter("ingest.digest.enqueue.errors").increment(result.errors.toDouble())
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

    private data class DigestTriggerResult(
        val enqueued: Int,
        val skipped: Int,
        val errors: Int,
    ) {
        val complete = errors == 0
    }
}
