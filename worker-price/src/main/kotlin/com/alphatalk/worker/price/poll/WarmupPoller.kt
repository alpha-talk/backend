package com.alphatalk.worker.price.poll

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.calendar.MarketPhase
import com.alphatalk.worker.price.demand.DemandSource
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled

class WarmupPoller(
    private val demand: DemandSource,
    private val poller: RestPollingScheduler,
    private val calendar: MarketCalendar,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 55 8 * * MON-FRI", zone = "Asia/Seoul")
    fun warmUp() {
        if (calendar.phase() == MarketPhase.CLOSED) return
        val warmed = poller.pollSymbols(demand.targetSymbols())
        log.info("warmup polling done: symbols={}", warmed)
    }
}
