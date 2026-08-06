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
import org.springframework.scheduling.support.CronExpression
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@Component
@ConditionalOnProperty("alphatalk.ingest.digest.enabled", havingValue = "true", matchIfMissing = true)
class DigestTrigger(
    private val queue: DigestJobQueue,
    private val props: IngestProperties,
    private val meters: MeterRegistry,
    @param:Qualifier(IngestConfig.CATCH_UP_EXECUTOR_BEAN)
    private val catchUpExecutor: Executor,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val zone = ZoneId.of(props.digest.zone)
    private val cron = props.digest.cron
        .takeIf { it != Scheduled.CRON_DISABLED }
        ?.let(CronExpression::parse)
    private val catchUpRunning = AtomicBoolean()
    private val reconciledThrough = AtomicReference<LocalDate?>()

    @Scheduled(cron = "\${alphatalk.ingest.digest.cron:0 0 18 * * *}", zone = "\${alphatalk.ingest.digest.zone:Asia/Seoul}")
    fun trigger() {
        val now = ZonedDateTime.now(clock.withZone(zone))
        val date = lastFireAtOrBefore(now)?.toLocalDate() ?: now.toLocalDate()
        val result = triggerForResult(date)
        record(result)
        if (result.complete) markReconciled(date)
    }

    @EventListener(ApplicationReadyEvent::class)
    fun catchUpOnStartup() {
        if (!props.digest.catchUpOnStartup) return
        submitCatchUp()
    }

    @Scheduled(
        fixedDelayString = "\${alphatalk.ingest.digest.catch-up-reconcile-delay:1m}",
        initialDelayString = "\${alphatalk.ingest.digest.catch-up-reconcile-delay:1m}",
    )
    fun reconcileCatchUp() {
        if (!props.digest.catchUpOnStartup || cron == null) return
        submitCatchUp()
    }

    private fun submitCatchUp() {
        if (!catchUpRunning.compareAndSet(false, true)) return
        runCatching {
            catchUpExecutor.execute {
                try {
                    catchUp()
                } catch (failure: Exception) {
                    meters.counter("ingest.digest.catchup.errors").increment()
                    log.warn("digest catch-up failed", failure)
                } finally {
                    catchUpRunning.set(false)
                }
            }
        }.onFailure {
            catchUpRunning.set(false)
            meters.counter("ingest.digest.catchup.errors").increment()
            log.warn("digest catch-up submission failed", it)
        }
    }

    private fun catchUp() {
        val now = ZonedDateTime.now(clock.withZone(zone))
        val missed = lastFireAtOrBefore(now)
        if (missed == null) {
            log.info("digest catch-up skipped: no elapsed schedule now={}", now)
            return
        }
        val date = missed.toLocalDate()
        if (reconciledThrough.get()?.let { !it.isBefore(date) } == true) return
        meters.counter("ingest.digest.catchup.attempts").increment()
        log.info("digest catch-up start: firedAt={} now={}", missed, now)
        val result = triggerForResult(date)
        record(result)
        if (result.complete) {
            markReconciled(date)
            meters.counter("ingest.digest.catchup.completed").increment()
        } else {
            meters.counter("ingest.digest.catchup.incomplete").increment()
        }
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

    private fun markReconciled(date: LocalDate) {
        reconciledThrough.updateAndGet { current ->
            if (current == null || date.isAfter(current)) date else current
        }
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

    private data class DigestTriggerResult(
        val enqueued: Int,
        val skipped: Int,
        val errors: Int,
    ) {
        val complete = errors == 0
    }
}
