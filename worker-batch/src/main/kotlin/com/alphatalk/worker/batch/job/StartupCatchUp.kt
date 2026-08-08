package com.alphatalk.worker.batch.job

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import java.time.Duration
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread

class StartupCatchUp(
    private val tasks: List<CatchUpTask>,
    private val meters: MeterRegistry,
    private val passes: Int = 3,
    private val passInterval: Duration = Duration.ofMinutes(20),
    private val stopTimeout: Duration = Duration.ofSeconds(5),
    private val now: () -> ZonedDateTime = { ZonedDateTime.now(SEOUL) },
    private val sleep: (Duration) -> Unit = { Thread.sleep(it.toMillis()) },
) : DisposableBean {
    private val log = LoggerFactory.getLogger(javaClass)
    private val started = AtomicBoolean(false)
    private val stopped = AtomicBoolean(false)

    @Volatile
    private var worker: Thread? = null

    init {
        require(passes >= 1) { "alphatalk.batch.catch-up.passes는 1 이상이어야 한다: $passes" }
        require(passInterval > Duration.ZERO) {
            "alphatalk.batch.catch-up.pass-interval은 양수여야 한다: $passInterval"
        }
    }

    fun registeredJobs(): List<String> = tasks.map(CatchUpTask::jobName)

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        if (tasks.isEmpty() || stopped.get()) return
        if (!started.compareAndSet(false, true)) return
        val runner = thread(start = false, name = "batch-catch-up", isDaemon = true) { runPasses() }
        worker = runner
        runner.start()
    }

    override fun destroy() {
        stopped.set(true)
        worker?.join(stopTimeout.toMillis().coerceAtLeast(1))
    }

    internal fun runPasses() {
        repeat(passes) { pass ->
            val result = runPending()
            if (result.attempted.isEmpty() || pass == passes - 1) return
            if (stopped.get() || Thread.currentThread().isInterrupted) return
            log.info("catch-up re-checking in {} - a triggered job may have been skipped by a stale lock", passInterval)
            if (!pause()) return
        }
    }

    internal fun runPending(): PassResult {
        val attempted = mutableListOf<String>()
        val succeeded = mutableListOf<String>()
        for (task in tasks) {
            if (stopped.get()) {
                log.warn("catch-up stopping, remaining jobs deferred to next boot: pending={}", pendingFrom(task))
                break
            }
            try {
                if (!windowPassed(task, now())) {
                    log.info("catch-up skipped, today's window not reached yet: job={}", task.jobName)
                    continue
                }
                log.info("catch-up triggering job whose window already passed: job={}", task.jobName)
                attempted += task.jobName
                task.run()
                succeeded += task.jobName
                meters.counter("batch.catchup.triggered", "job", task.jobName).increment()
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                log.warn("catch-up interrupted mid-job, deferred to next boot: job={}", task.jobName)
                break
            } catch (e: Exception) {
                meters.counter("batch.catchup.failed", "job", task.jobName).increment()
                log.error("catch-up failed - next pass or scheduled run retries: job={}", task.jobName, e)
            }
        }
        return PassResult(attempted, succeeded)
    }

    private fun pause(): Boolean {
        var remaining = passInterval.toMillis()
        while (remaining > 0) {
            if (stopped.get()) return false
            val slice = minOf(SLEEP_SLICE_MILLIS, remaining)
            try {
                sleep(Duration.ofMillis(slice))
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return false
            }
            remaining -= slice
        }
        return !stopped.get()
    }

    private fun pendingFrom(task: CatchUpTask): List<String> =
        tasks.dropWhile { it !== task }.map(CatchUpTask::jobName)

    private fun windowPassed(task: CatchUpTask, current: ZonedDateTime): Boolean {
        if (task.cron.trim() == Scheduled.CRON_DISABLED) return false
        val expression = CronExpression.parse(task.cron.trim())
        val beforeMidnight = current.toLocalDate().atStartOfDay(current.zone).minusNanos(1)
        val firstToday = expression.next(beforeMidnight)
        return firstToday != null && firstToday.toLocalDate() == current.toLocalDate() && !firstToday.isAfter(current)
    }

    internal data class PassResult(
        val attempted: List<String>,
        val succeeded: List<String>,
    )

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
        private const val SLEEP_SLICE_MILLIS = 500L
    }
}
