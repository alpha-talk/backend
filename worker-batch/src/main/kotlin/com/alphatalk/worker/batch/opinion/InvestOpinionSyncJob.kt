package com.alphatalk.worker.batch.opinion

import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.core.LockConfiguration
import net.javacrumbs.shedlock.core.LockProvider
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.DayOfWeek
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

open class InvestOpinionSyncJob(
    private val brokers: BrokerDirectory,
    private val fetcher: OpinionFetcher,
    private val store: InvestOpinionStore,
    private val binder: OpinionEventBinder,
    private val publisher: OpinionPublisher,
    private val runs: BatchJobRunStore,
    private val locks: LockProvider,
    private val meters: MeterRegistry,
    private val holidays: Set<LocalDate> = emptySet(),
    private val businessDayEnforced: Boolean = true,
    private val requestInterval: Duration = Duration.ofMillis(250),
    private val scanLimit: Int = 500,
    private val clock: () -> Instant = Instant::now,
    private val pause: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "\${alphatalk.batch.opinion.cron:0 0/10 7-17 * * MON-FRI}", zone = "Asia/Seoul")
    open fun scheduled() {
        syncOnce()
    }

    open fun syncOnce(): Int {
        val today = LocalDate.ofInstant(clock(), SEOUL)
        if (businessDayEnforced && !isBusinessDay(today)) return 0
        val lock = locks.lock(LockConfiguration(clock(), JOB_NAME, LOCK_AT_MOST_FOR, LOCK_AT_LEAST_FOR)).orElse(null)
        if (lock == null) {
            meters.counter("batch.opinion.overrun").increment()
            log.warn("opinion_sync_overrun: previous cycle still running, skipping")
            return 0
        }
        try {
            return run(today)
        } finally {
            lock.unlock()
        }
    }

    private fun run(today: LocalDate): Int {
        val runId = runs.restart(JOB_NAME, today.format(DateTimeFormatter.BASIC_ISO_DATE), clock())
        try {
            val started = clock()
            val (inserted, failed) = collect(today, deadline = started.plus(COLLECT_DEADLINE))
            val published = publishPending(deadline = started.plus(PUBLISH_DEADLINE))
            meters.counter("batch.opinion.published").increment(published.toDouble())
            runs.succeed(runId, inserted, failed, clock())
            log.info("opinion sync done: inserted={} published={} brokerFailures={}", inserted, published, failed)
            return published
        } catch (e: Exception) {
            runs.fail(runId, e.toString(), clock())
            throw e
        }
    }

    private fun collect(today: LocalDate, deadline: Instant): Pair<Int, Int> {
        val from = previousBusinessDay(today)
        val members = brokers.brokers()
        if (members.isEmpty()) return 0 to 0
        val start = ((clock().epochSecond / CYCLE_SECONDS) % members.size).toInt()
        var inserted = 0
        val retryQueue = mutableListOf<Broker>()
        for (offset in members.indices) {
            if (clock() >= deadline) {
                meters.counter("batch.opinion.deadline").increment()
                log.warn(
                    "collect deadline reached before lock expiry, deferring {} brokers to next cycle",
                    members.size - offset,
                )
                break
            }
            val broker = members[(start + offset) % members.size]
            val collected = fetchAndStore(broker, from, today)
            if (collected == null) {
                log.warn("opinion fetch failed, retrying at end of cycle: broker={}", broker.code)
                retryQueue += broker
            } else {
                inserted += collected
            }
        }
        var failed = 0
        for ((index, broker) in retryQueue.withIndex()) {
            if (clock() >= deadline) {
                failed += retryQueue.size - index
                meters.counter("batch.opinion.deadline").increment()
                log.warn(
                    "collect deadline reached before retry pass completed, deferring {} brokers to next cycle",
                    retryQueue.size - index,
                )
                break
            }
            val collected = fetchAndStore(broker, from, today)
            if (collected == null) {
                failed++
                log.warn("opinion retry failed, next cycle retries: broker={}", broker.code)
            } else {
                inserted += collected
            }
        }
        meters.counter("batch.opinion.collected").increment(inserted.toDouble())
        return inserted to failed
    }

    private fun fetchAndStore(broker: Broker, from: LocalDate, to: LocalDate): Int? {
        pause(requestInterval)
        val rows = try {
            fetcher.fetch(broker, from, to)
        } catch (e: Exception) {
            log.warn("opinion fetch failed: broker={}", broker.code, e)
            return null
        }
        if (rows.size >= KisRestClient.INVEST_OPINION_PAGE_CAP) {
            meters.counter("batch.opinion.truncated").increment()
            log.warn("opinion response hit page cap, oldest rows may be missing: broker={} rows={}", broker.code, rows.size)
        }
        var inserted = 0
        for (row in rows) {
            val observation = OpinionObservation(
                code = row.code,
                businessDate = row.businessDate,
                brokerCode = broker.code,
                brokerName = row.memberName ?: broker.name.ifBlank { null },
                rating = row.rating,
                previousRating = row.previousRating,
                targetPrice = row.targetPrice,
                contentHash = OpinionObservation.contentHash(row.rating, row.previousRating, row.targetPrice),
                collectedAt = clock(),
            )
            if (store.insertIfAbsent(observation)) inserted++
        }
        return inserted
    }

    internal fun publishPending(deadline: Instant): Int {
        var published = 0
        val pendings = store.findUnpublished(scanLimit)
        for ((index, pending) in pendings.withIndex()) {
            if (clock() >= deadline) {
                meters.counter("batch.opinion.deadline").increment()
                log.warn(
                    "publish deadline reached before lock expiry, deferring {} events to next cycle",
                    pendings.size - index,
                )
                break
            }
            val eventId = binder.ensureEvent(pending)
            if (publisher.publish(pending.observation.code, eventId, pending.observation.toStreamData())) {
                store.markPublished(eventId, clock())
                published++
            }
        }
        return published
    }

    private fun isBusinessDay(date: LocalDate): Boolean =
        date.dayOfWeek != DayOfWeek.SATURDAY && date.dayOfWeek != DayOfWeek.SUNDAY && date !in holidays

    private fun previousBusinessDay(date: LocalDate): LocalDate {
        var cursor = date.minusDays(1)
        while (!isBusinessDay(cursor)) {
            cursor = cursor.minusDays(1)
        }
        return cursor
    }

    companion object {
        const val JOB_NAME = "invest_opinion_sync"
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private val LOCK_AT_MOST_FOR: Duration = Duration.ofMinutes(15)
        private val LOCK_AT_LEAST_FOR: Duration = Duration.ofSeconds(5)
        private val COLLECT_DEADLINE: Duration = Duration.ofMinutes(6)
        private val PUBLISH_DEADLINE: Duration = Duration.ofMinutes(8)
        private const val CYCLE_SECONDS = 600L
    }
}
