package com.alphatalk.worker.price.poll

import com.alphatalk.contracts.envelope.QuoteData
import com.alphatalk.kis.rest.KisQuoteSnapshot
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.demand.FixedDemandSource
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

    private fun poller(calendar: MarketCalendar): WarmupPoller {
        val scheduler = RestPollingScheduler(
            degraded = { emptySet() },
            fetcher = { code ->
                KisQuoteSnapshot(code, 71200, 700, 0.99, 70600, 71500, 70400, 1234567)
            },
            publisher = publisher,
            calendar = calendar,
            meters = SimpleMeterRegistry(),
        )
        return WarmupPoller(FixedDemandSource(listOf("005930", "000660")), scheduler, calendar)
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

    private class RecordingPublisher : QuotePublisher {
        val published = mutableListOf<Pair<String, QuoteData>>()

        override fun publish(code: String, data: QuoteData, ts: Long) {
            published += code to data
        }
    }
}
