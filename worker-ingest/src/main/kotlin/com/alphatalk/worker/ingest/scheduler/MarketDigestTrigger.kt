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
@ConditionalOnProperty("alphatalk.ingest.digest.market-enabled", havingValue = "true")
class MarketDigestTrigger(
    private val queue: DigestJobQueue,
    private val props: IngestProperties,
    private val meters: MeterRegistry,
    @param:Qualifier(IngestConfig.CATCH_UP_EXECUTOR_BEAN)
    catchUpExecutor: Executor,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val catchUp = DigestCatchUp(
        cronExpression = props.digest.marketCron,
        zoneId = props.digest.zone,
        clock = clock,
        executor = catchUpExecutor,
        meters = meters,
        metricPrefix = "ingest.digest.market.catchup",
        action = ::triggerFor,
    )

    @Scheduled(cron = "\${alphatalk.ingest.digest.market-cron:0 40 17 * * *}", zone = "\${alphatalk.ingest.digest.zone:Asia/Seoul}")
    fun trigger() {
        val date = catchUp.fireDate()
        if (triggerFor(date)) catchUp.markReconciled(date)
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

    fun triggerFor(date: LocalDate): Boolean = runCatching {
        when (queue.enqueueIfNew(entryOf(date))) {
            DigestEnqueueResult.ENQUEUED -> meters.counter("ingest.digest.market.enqueued").increment()
            DigestEnqueueResult.ALREADY_ENQUEUED -> meters.counter("ingest.digest.market.duplicate.skipped").increment()
        }
        log.info("market digest job enqueued: date={}", date)
        true
    }.getOrElse {
        meters.counter("ingest.digest.market.enqueue.errors").increment()
        log.warn("market digest job enqueue failed: date={}", date, it)
        false
    }

    private fun entryOf(date: LocalDate) =
        IngestQueueEntry(
            source = IngestQueueEntry.DIGEST_SOURCE,
            sourceId = IngestQueueEntry.digestSourceId(IngestQueueEntry.MARKET_CODE, date.toString()),
            type = IngestType.DIGEST,
            codes = listOf(IngestQueueEntry.MARKET_CODE),
            title = "",
            url = "",
            fetchedAt = clock.millis(),
        )
}
