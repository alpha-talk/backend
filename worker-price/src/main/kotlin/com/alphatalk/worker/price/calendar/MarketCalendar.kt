package com.alphatalk.worker.price.calendar

import java.time.DayOfWeek
import java.time.Instant
import java.time.LocalDate
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime

enum class MarketPhase { CLOSED, PREPARE, OPEN }

class MarketCalendar(
    private val holidays: Set<LocalDate> = emptySet(),
    private val enforced: Boolean = true,
    private val zone: ZoneId = ZoneId.of("Asia/Seoul"),
    private val clock: () -> Instant = Instant::now,
) {
    fun phase(): MarketPhase {
        if (!enforced) return MarketPhase.OPEN
        val now = ZonedDateTime.ofInstant(clock(), zone)
        if (!isBusinessDay(now.toLocalDate())) return MarketPhase.CLOSED
        val time = now.toLocalTime()
        return when {
            time >= OPEN_START && time < CLOSE_END -> MarketPhase.OPEN
            time >= PREPARE_START && time < OPEN_START -> MarketPhase.PREPARE
            else -> MarketPhase.CLOSED
        }
    }

    fun isTradingDay(): Boolean {
        if (!enforced) return true
        return isBusinessDay(ZonedDateTime.ofInstant(clock(), zone).toLocalDate())
    }

    private fun isBusinessDay(date: LocalDate): Boolean =
        date.dayOfWeek != DayOfWeek.SATURDAY && date.dayOfWeek != DayOfWeek.SUNDAY && date !in holidays

    companion object {
        private val PREPARE_START = LocalTime.of(7, 50)
        private val OPEN_START = LocalTime.of(8, 0)
        private val CLOSE_END = LocalTime.of(20, 0)
    }
}
