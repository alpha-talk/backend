package com.alphatalk.worker.batch.job

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.scheduling.support.CronExpression
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.concurrent.thread

class StartupCatchUp(
    private val tasks: List<CatchUpTask>,
    private val meters: MeterRegistry,
    private val now: () -> ZonedDateTime = { ZonedDateTime.now(SEOUL) },
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun registeredJobs(): List<String> = tasks.map(CatchUpTask::jobName)

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        if (tasks.isEmpty()) return
        thread(name = "batch-catch-up", isDaemon = true) { runPending() }
    }

    internal fun runPending(): List<String> {
        val executed = mutableListOf<String>()
        tasks.forEach { task ->
            try {
                if (!windowPassed(task, now())) {
                    log.info("catch-up skipped, today's window not reached yet: job={}", task.jobName)
                    return@forEach
                }
                log.info("catch-up triggering job whose window already passed: job={}", task.jobName)
                task.run()
                executed += task.jobName
                meters.counter("batch.catchup.triggered", "job", task.jobName).increment()
            } catch (e: Exception) {
                meters.counter("batch.catchup.failed", "job", task.jobName).increment()
                log.error("catch-up failed - next scheduled run retries: job={}", task.jobName, e)
            }
        }
        return executed
    }

    private fun windowPassed(task: CatchUpTask, current: ZonedDateTime): Boolean {
        if (task.cron.trim() == Scheduled.CRON_DISABLED) return false
        val expression = CronExpression.parse(task.cron.trim())
        val beforeMidnight = current.toLocalDate().atStartOfDay(current.zone).minusNanos(1)
        val firstToday = expression.next(beforeMidnight)
        return firstToday != null && firstToday.toLocalDate() == current.toLocalDate() && !firstToday.isAfter(current)
    }

    companion object {
        private val SEOUL: ZoneId = ZoneId.of("Asia/Seoul")
    }
}
