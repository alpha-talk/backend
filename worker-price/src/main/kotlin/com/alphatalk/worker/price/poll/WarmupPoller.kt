package com.alphatalk.worker.price.poll

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.calendar.MarketPhase
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled

class WarmupPoller(
    private val demand: DemandSource,
    private val poller: RestPollingScheduler,
    private val calendar: MarketCalendar,
    private val leader: LeaderLock,
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(cron = "0 55 7 * * MON-FRI", zone = "Asia/Seoul")
    fun warmUp() {
        if (!leader.tryAcquire()) return
        if (calendar.phase() == MarketPhase.CLOSED) return
        val warmed = poller.pollSymbols(demand.targetSymbols())
        log.info("warmup polling done: symbols={}", warmed)
    }
}
