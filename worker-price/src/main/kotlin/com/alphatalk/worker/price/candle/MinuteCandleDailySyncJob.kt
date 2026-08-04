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
    private val service: MinuteCandleRefreshService,
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
        var synced = 0
        (symbols() + store.codesOn(date)).forEach { code ->
            runCatching { service.refresh(code) }
                .onSuccess { synced += 1 }
                .onFailure { log.warn("minute candle daily sync failed: code={}", code, it) }
        }
        return synced
    }
}
