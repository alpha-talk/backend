package com.alphatalk.worker.ingest.scheduler

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Configuration
@EnableScheduling
class SchedulingConfig

@Component
@ConditionalOnProperty("alphatalk.ingest.poll-enabled", havingValue = "true", matchIfMissing = true)
class PollingScheduler(
    private val poller: IngestPoller,
    private val meters: MeterRegistry,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(
        fixedDelayString = "\${alphatalk.ingest.poll-delay:5m}",
        initialDelayString = "\${alphatalk.ingest.poll-initial-delay:10s}",
    )
    fun poll() {
        val stats = poller.pollOnce()
        meters.counter("ingest.fetched").increment(stats.fetched.toDouble())
        meters.counter("ingest.enqueued").increment(stats.enqueued.toDouble())
        meters.counter("ingest.dup.skipped").increment(stats.duplicateSkipped.toDouble())
        meters.counter("ingest.unmatched.skipped").increment(stats.unmatchedSkipped.toDouble())
        meters.counter("ingest.source.errors").increment(stats.sourceErrors.toDouble())
        meters.counter("ingest.enqueue.errors").increment(stats.enqueueErrors.toDouble())
        if (stats != PollStats()) {
            log.info(
                "poll done: fetched={} enqueued={} dup={} unmatched={} sourceErrors={} enqueueErrors={}",
                stats.fetched, stats.enqueued, stats.duplicateSkipped,
                stats.unmatchedSkipped, stats.sourceErrors, stats.enqueueErrors,
            )
        }
    }
}
