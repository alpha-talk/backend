package com.alphatalk.worker.ingest.scheduler

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import java.time.Clock
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

internal class DigestCatchUp(
    cronExpression: String,
    zoneId: String,
    private val clock: Clock,
    private val executor: Executor,
    private val meters: MeterRegistry,
    private val metricPrefix: String,
    private val action: (LocalDate) -> Boolean,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val zone = ZoneId.of(zoneId)
    private val cron = cronExpression
        .takeIf { it != Scheduled.CRON_DISABLED }
        ?.let(CronExpression::parse)
    private val running = AtomicBoolean()
    private val reconciledThrough = AtomicReference<LocalDate?>()

    fun fireDate(): LocalDate {
        val now = ZonedDateTime.now(clock.withZone(zone))
        return lastFireAtOrBefore(now)?.toLocalDate() ?: now.toLocalDate()
    }

    fun markReconciled(date: LocalDate) {
        reconciledThrough.updateAndGet { current ->
            if (current == null || date.isAfter(current)) date else current
        }
    }

    fun submit() {
        if (cron == null) return
        if (!running.compareAndSet(false, true)) return
        runCatching {
            executor.execute {
                try {
                    catchUpNow()
                } catch (failure: Exception) {
                    meters.counter("$metricPrefix.errors").increment()
                    log.warn("digest catch-up failed: job={}", metricPrefix, failure)
                } finally {
                    running.set(false)
                }
            }
        }.onFailure {
            running.set(false)
            meters.counter("$metricPrefix.errors").increment()
            log.warn("digest catch-up submission failed: job={}", metricPrefix, it)
        }
    }

    private fun catchUpNow() {
        val now = ZonedDateTime.now(clock.withZone(zone))
        val missed = lastFireAtOrBefore(now)
        if (missed == null) {
            log.info("digest catch-up skipped: no elapsed schedule job={} now={}", metricPrefix, now)
            return
        }
        val date = missed.toLocalDate()
        if (reconciledThrough.get()?.let { !it.isBefore(date) } == true) return
        meters.counter("$metricPrefix.attempts").increment()
        log.info("digest catch-up start: job={} firedAt={} now={}", metricPrefix, missed, now)
        if (action(date)) {
            markReconciled(date)
            meters.counter("$metricPrefix.completed").increment()
        } else {
            meters.counter("$metricPrefix.incomplete").increment()
        }
    }

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
