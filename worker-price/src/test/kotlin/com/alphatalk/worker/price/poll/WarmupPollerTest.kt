package com.alphatalk.worker.price.poll

import com.alphatalk.contracts.envelope.QuoteData
import com.alphatalk.kis.rest.KisQuoteSnapshot
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.publish.QuotePublisher
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class WarmupPollerTest {
    private val publisher = RecordingPublisher()

    private fun at(day: Int, hour: Int, minute: Int): () -> Instant =
        { ZonedDateTime.of(2026, 7, day, hour, minute, 0, 0, ZoneId.of("Asia/Seoul")).toInstant() }

    private fun poller(calendar: MarketCalendar, leader: LeaderLock = ToggleLeaderLock(leader = true)): WarmupPoller {
        val scheduler = RestPollingScheduler(
            degraded = { emptySet() },
            fetcher = { code, _ ->
                KisQuoteSnapshot(code, 71200, 700, 0.99, 70600, 71500, 70400, 1234567)
            },
            marketDivs = com.alphatalk.worker.price.market.InMemoryMarketDivStore(),
            publisher = publisher,
            calendar = calendar,
            leader = leader,
            meters = SimpleMeterRegistry(),
        )
        return WarmupPoller(DemandSource { setOf("005930", "000660") }, scheduler, calendar, leader)
    }

    @Test
    fun `장 준비 시간에는 수요 종목 전체를 워밍한다`() {
        poller(MarketCalendar(clock = at(27, 8, 55))).warmUp()

        assertEquals(setOf("005930", "000660"), publisher.published.map { it.first }.toSet())
    }

    @Test
    fun `휴장일에는 워밍하지 않는다`() {
        poller(MarketCalendar(clock = at(26, 8, 55))).warmUp()

        assertEquals(0, publisher.published.size)
    }

    @Test
    fun `스탠바이 인스턴스는 워밍하지 않는다`() {
        poller(MarketCalendar(clock = at(27, 8, 55)), ToggleLeaderLock(leader = false)).warmUp()

        assertEquals(0, publisher.published.size)
    }

    private class RecordingPublisher : QuotePublisher {
        val published = mutableListOf<Pair<String, QuoteData>>()

        override fun publish(code: String, data: QuoteData, ts: Long): Boolean {
            published += code to data
            return true
        }
    }

    private class ToggleLeaderLock(var leader: Boolean) : LeaderLock {
        override fun tryAcquire(): Boolean = leader

        override fun release() {
        }
    }
}
