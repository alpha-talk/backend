package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestConfig
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.DigestEnqueueResult
import com.alphatalk.worker.ingest.queue.DigestJobQueue
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
import java.util.concurrent.Executor

@Component
@ConditionalOnProperty("alphatalk.ingest.digest.enabled", havingValue = "true", matchIfMissing = true)
class DigestTrigger(
    private val queue: DigestJobQueue,
    private val props: IngestProperties,
    private val meters: MeterRegistry,
    @param:Qualifier(IngestConfig.CATCH_UP_EXECUTOR_BEAN)
    catchUpExecutor: Executor,
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
        var enqueued = 0
        var skipped = 0
        var errors = 0
        props.stocks.forEach { stock ->
            val sourceId = IngestQueueEntry.digestSourceId(stock.code, date.toString())
            runCatching { queue.enqueueIfNew(entryOf(stock.code, sourceId)) }
                .onSuccess { result ->
                    when (result) {
                        DigestEnqueueResult.ENQUEUED -> enqueued++
                        DigestEnqueueResult.ALREADY_ENQUEUED -> skipped++
                    }
                }
                .onFailure {
                    errors++
                    log.warn("digest job enqueue failed: code={}", stock.code, it)
                }
        }
        log.info(
            "digest jobs enqueued: {}/{} skipped={} errors={} date={}",
            enqueued, props.stocks.size, skipped, errors, date,
        )
        return DigestTriggerResult(enqueued, skipped, errors)
    }

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
