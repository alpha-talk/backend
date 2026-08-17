package com.alphatalk.worker.price.session

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.calendar.MarketPhase
import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.stereotype.Component

@Component
@ConditionalOnKisAccounts
class PriceOrchestrator(
    private val demand: DemandSource,
    private val pool: SessionPool,
    private val calendar: MarketCalendar,
    private val leader: LeaderLock,
    meters: MeterRegistry,
) {
    @Volatile
    private var demandCount = 0

    init {
        Gauge.builder("demand.symbols", this) { it.demandCount.toDouble() }.register(meters)
    }

    fun tick() {
        if (!leader.tryAcquire()) {
            pool.disconnectAll()
            return
        }
        val target = demand.targetSymbols()
        demandCount = target.size
        when (calendar.phase()) {
            MarketPhase.CLOSED -> pool.disconnectAll()
            MarketPhase.PREPARE -> pool.maintain(target, subscribeAllowed = false)
            MarketPhase.OPEN -> pool.maintain(target, subscribeAllowed = true)
        }
    }

    fun shutdown() {
        pool.disconnectAll()
        leader.release()
    }
}
