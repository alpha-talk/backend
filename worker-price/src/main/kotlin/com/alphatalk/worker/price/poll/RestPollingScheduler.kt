package com.alphatalk.worker.price.poll

import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.calendar.MarketPhase
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.publish.QuotePublisher
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import java.time.Clock

class RestPollingScheduler(
    private val degraded: () -> Set<String>,
    private val fetcher: QuoteSnapshotFetcher,
    private val publisher: QuotePublisher,
    private val calendar: MarketCalendar,
    private val leader: LeaderLock,
    private val meters: MeterRegistry,
    private val clock: Clock = Clock.systemUTC(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${alphatalk.price.poll-interval-ms:30000}")
    fun poll() {
        if (!leader.tryAcquire()) return
        if (calendar.phase() != MarketPhase.OPEN) return
        pollSymbols(degraded())
    }

    fun pollSymbols(symbols: Set<String>): Int {
        var polled = 0
        symbols.forEach { code ->
            runCatching {
                val snapshot = fetcher.fetch(code)
                publisher.publish(code, snapshot.toQuoteData(), clock.millis())
                polled += 1
            }.onFailure {
                log.warn("rest poll failed: code={}", code, it)
            }
        }
        if (polled > 0) {
            meters.counter("rest.poll").increment(polled.toDouble())
        }
        return polled
    }
}
