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
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 0 16 * * MON-FRI", zone = "Asia/Seoul")
    fun syncDaily() {
        if (!calendar.isTradingDay()) return
        if (!leader.tryAcquire()) return
        syncOnce()
    }

    fun syncOnce(): Int {
        val date = today().format(DateTimeFormatter.BASIC_ISO_DATE)
        var completed = 0
        (symbols() + store.codesOn(date)).forEach { code ->
            runCatching {
                if (!isDayComplete(code, date)) syncDay(code)
                if (!isDayComplete(code, date)) syncDay(code)
                if (isDayComplete(code, date)) {
                    completed += 1
                } else {
                    log.warn(
                        "minute candle daily sync incomplete: code={} date={} latest={}",
                        code,
                        date,
                        store.latestTime(code, date),
                    )
                }
            }.onFailure {
                log.warn("minute candle daily sync failed: code={}", code, it)
            }
        }
        return completed
    }
}
