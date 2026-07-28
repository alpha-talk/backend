package com.alphatalk.worker.price.calendar

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZonedDateTime
import kotlin.test.Test
import kotlin.test.assertEquals

class MarketCalendarTest {
    private val seoul = ZoneId.of("Asia/Seoul")

    private fun at(year: Int, month: Int, day: Int, hour: Int, minute: Int): () -> Instant =
        { ZonedDateTime.of(year, month, day, hour, minute, 0, 0, seoul).toInstant() }

    @Test
    fun `평일 장중 구간별 phase`() {
        assertEquals(MarketPhase.CLOSED, MarketCalendar(clock = at(2026, 7, 27, 8, 49)).phase())
        assertEquals(MarketPhase.PREPARE, MarketCalendar(clock = at(2026, 7, 27, 8, 55)).phase())
        assertEquals(MarketPhase.OPEN, MarketCalendar(clock = at(2026, 7, 27, 9, 0)).phase())
        assertEquals(MarketPhase.OPEN, MarketCalendar(clock = at(2026, 7, 27, 15, 39)).phase())
        assertEquals(MarketPhase.CLOSED, MarketCalendar(clock = at(2026, 7, 27, 15, 40)).phase())
    }

    @Test
    fun `주말은 CLOSED다`() {
        assertEquals(MarketPhase.CLOSED, MarketCalendar(clock = at(2026, 7, 26, 10, 0)).phase())
        assertEquals(MarketPhase.CLOSED, MarketCalendar(clock = at(2026, 7, 25, 10, 0)).phase())
    }

    @Test
    fun `휴장일은 CLOSED다`() {
        val calendar = MarketCalendar(
            holidays = setOf(LocalDate.of(2026, 7, 27)),
            clock = at(2026, 7, 27, 10, 0),
        )
        assertEquals(MarketPhase.CLOSED, calendar.phase())
    }

    @Test
    fun `강제 해제 시 항상 OPEN이다`() {
        val calendar = MarketCalendar(enforced = false, clock = at(2026, 7, 26, 3, 0))
        assertEquals(MarketPhase.OPEN, calendar.phase())
    }

    @Test
    fun `거래일 여부는 주말·휴장일만 거른다`() {
        assertEquals(true, MarketCalendar(clock = at(2026, 7, 27, 16, 30)).isTradingDay())
        assertEquals(false, MarketCalendar(clock = at(2026, 7, 26, 16, 30)).isTradingDay())
        assertEquals(
            false,
            MarketCalendar(
                holidays = setOf(java.time.LocalDate.of(2026, 7, 27)),
                clock = at(2026, 7, 27, 16, 30),
            ).isTradingDay(),
        )
        assertEquals(true, MarketCalendar(enforced = false, clock = at(2026, 7, 26, 16, 30)).isTradingDay())
    }
}
