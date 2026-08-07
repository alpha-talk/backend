package com.alphatalk.worker.price.poll

import com.alphatalk.contracts.envelope.QuoteData
import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.rest.KisQuoteSnapshot
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.publish.QuotePublisher
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import com.alphatalk.worker.price.market.InMemoryMarketDivStore
import kotlin.test.assertEquals

class RestPollingSchedulerTest {
    private fun snapshot(code: String) = KisQuoteSnapshot(
        code = code,
        price = 71200,
        change = 700,
        changeRate = 0.99,
        open = 70600,
        high = 71500,
        low = 70400,
        volume = 1234567,
    )

    private fun scheduler(
        symbols: Set<String>,
        publisher: QuotePublisher,
        fetcher: QuoteSnapshotFetcher = QuoteSnapshotFetcher { code, _ -> snapshot(code) },
        calendar: MarketCalendar = MarketCalendar(enforced = false),
        leader: LeaderLock = ToggleLeaderLock(leader = true),
    ) = RestPollingScheduler(
        degraded = { symbols },
        fetcher = fetcher,
        marketDivs = InMemoryMarketDivStore(),
        publisher = publisher,
        calendar = calendar,
        leader = leader,
        meters = SimpleMeterRegistry(),
        clock = Clock.fixed(Instant.ofEpochMilli(1719500000000), ZoneId.of("UTC")),
    )

    @Test
    fun `강등 심볼을 스냅샷으로 조회해 동일 발행 경로로 내보낸다`() {
        val publisher = RecordingPublisher()
        val scheduler = scheduler(linkedSetOf("005930", "000660"), publisher)

        scheduler.poll()

        assertEquals(2, publisher.published.size)
        val (code, data, ts) = publisher.published[0]
        assertEquals("005930", code)
        assertEquals(70500, data.prevClose)
        assertEquals(1719500000000, ts)
    }

    @Test
    fun `한 종목이 실패해도 나머지는 발행된다`() {
        val publisher = RecordingPublisher()
        val fetcher = QuoteSnapshotFetcher { code, _ ->
            if (code == "005930") throw KisClientException("boom")
            snapshot(code)
        }
        val scheduler = scheduler(linkedSetOf("005930", "000660"), publisher, fetcher)

        val polled = scheduler.pollSymbols(linkedSetOf("005930", "000660"))

        assertEquals(1, polled)
        assertEquals(listOf("000660"), publisher.published.map { it.first })
    }

    @Test
    fun `장이 닫혀 있으면 폴링하지 않는다`() {
        val publisher = RecordingPublisher()
        val sunday = { ZonedDateTime.of(2026, 7, 26, 10, 0, 0, 0, ZoneId.of("Asia/Seoul")).toInstant() }
        val scheduler = scheduler(setOf("005930"), publisher, calendar = MarketCalendar(clock = sunday))

        scheduler.poll()

        assertEquals(0, publisher.published.size)
    }

    @Test
    fun `리더만 폴링하고 스탠바이는 건너뛴다`() {
        val leaderPublisher = RecordingPublisher()
        val standbyPublisher = RecordingPublisher()
        val leaderInstance = scheduler(setOf("005930"), leaderPublisher, leader = ToggleLeaderLock(leader = true))
        val standbyInstance = scheduler(setOf("005930"), standbyPublisher, leader = ToggleLeaderLock(leader = false))

        leaderInstance.poll()
        standbyInstance.poll()

        assertEquals(1, leaderPublisher.published.size)
        assertEquals(0, standbyPublisher.published.size)
    }

    private class RecordingPublisher : QuotePublisher {
        val published = mutableListOf<Triple<String, QuoteData, Long>>()

        override fun publish(code: String, data: QuoteData, ts: Long) {
            published += Triple(code, data, ts)
        }
    }

    private class ToggleLeaderLock(var leader: Boolean) : LeaderLock {
        override fun tryAcquire(): Boolean = leader

        override fun release() {
        }
    }
}
