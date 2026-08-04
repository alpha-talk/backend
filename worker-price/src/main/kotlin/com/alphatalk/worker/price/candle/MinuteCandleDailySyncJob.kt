package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.leader.LeaderLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

class MinuteCandleDailySyncJob(
    private val symbols: () -> Set<String>,
    private val store: MinuteCandleStore,
    private val syncDay: (String) -> Int,
    private val isDayComplete: (String, String) -> Boolean,
    private val calendar: MarketCalendar,
    private val leader: LeaderLock,
    private val today: () -> LocalDate = { LocalDate.now(ZoneId.of("Asia/Seoul")) },
    private val sleeper: (Long) -> Unit = Thread::sleep,
    private val budgetMillis: Long = DEFAULT_BUDGET_MILLIS,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 0 16 * * MON-FRI", zone = "Asia/Seoul")
    fun syncDaily() {
        if (!calendar.isTradingDay()) return
        if (!leader.tryAcquire()) return
        attemptSync()
    }

    @Scheduled(cron = "0 15 17,18,19 * * MON-FRI", zone = "Asia/Seoul")
    fun retryUnfinished() {
        if (!calendar.isTradingDay()) return
        if (!leader.tryAcquire()) return
        attemptSync()
    }

    private fun attemptSync() {
        val result = try {
            syncOnce()
        } catch (e: Exception) {
            log.error("minute candle daily sync aborted - 다음 회차에 재시도한다", e)
            return
        }
        if (result.complete) return
        log.error(
            "minute candle daily sync incomplete - 다음 회차에 재시도한다. completed={} incomplete={}",
            result.completed,
            result.incomplete,
        )
    }

    fun syncOnce(): MinuteDailySyncResult {
        val date = today().format(DateTimeFormatter.BASIC_ISO_DATE)
        val targets = symbols() + store.codesOn(date)
        val pending = targets.filterNot { isDayComplete(it, date) }
        var completed = targets.size - pending.size
        if (pending.isEmpty()) return MinuteDailySyncResult(completed = completed, incomplete = 0)

        log.info("minute candle daily sync: date={} targets={} pending={}", date, targets.size, pending.size)
        val startedAt = System.nanoTime()
        var incomplete = 0
        var skipped = 0
        pending.forEach { code ->
            if (elapsedMillis(startedAt) >= budgetMillis) {
                skipped += 1
                return@forEach
            }
            var attempts = 0
            while (!isDayComplete(code, date) && attempts < MAX_ATTEMPTS_PER_CODE) {
                attempts += 1
                runCatching { syncDay(code) }.onFailure {
                    log.warn("minute candle daily sync attempt failed: code={} attempt={}", code, attempts, it)
                }
                if (!isDayComplete(code, date) && attempts < MAX_ATTEMPTS_PER_CODE) {
                    sleeper(BACKOFF_MILLIS * attempts)
                }
            }
            if (isDayComplete(code, date)) {
                completed += 1
            } else {
                incomplete += 1
                log.warn(
                    "minute candle daily sync incomplete: code={} date={} latest={}",
                    code,
                    date,
                    store.latestTime(code, date),
                )
            }
        }
        if (skipped > 0) {
            log.warn("minute candle daily sync budget exhausted - 다음 회차로 넘긴다. skipped={}", skipped)
        }
        return MinuteDailySyncResult(completed = completed, incomplete = incomplete + skipped)
    }

    private fun elapsedMillis(startedAtNanos: Long): Long = (System.nanoTime() - startedAtNanos) / 1_000_000

    companion object {
        private const val MAX_ATTEMPTS_PER_CODE = 3
        private const val BACKOFF_MILLIS = 1_000L
        const val DEFAULT_BUDGET_MILLIS = 30 * 60_000L
    }
}

data class MinuteDailySyncResult(val completed: Int, val incomplete: Int) {
    val complete: Boolean get() = incomplete == 0
}
