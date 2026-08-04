package com.alphatalk.coreapi.stockinfo

import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.format.DateTimeFormatter

enum class MinutePeriod(val token: String, val unitMinutes: Int) {
    ONE("1m", 1),
    FIVE("5m", 5),
    FIFTEEN("15m", 15),
    THIRTY("30m", 30),
    SIXTY("60m", 60),
    ;

    companion object {
        fun fromToken(token: String): MinutePeriod? =
            entries.firstOrNull { it.token == token.trim().lowercase() }
    }
}

data class MinuteBucket(
    val date: String,
    val time: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)

data class MinuteWindow(
    val buckets: List<MinuteBucket>,
    val hasMoreBefore: Boolean,
)

object MinuteCandleAggregator {
    fun fetchLimit(period: MinutePeriod, count: Int): Int =
        minOf(count * period.unitMinutes + 2, MAX_FETCH_ROWS)

    fun aggregate(
        minutesDescending: List<MinuteCandleRow>,
        period: MinutePeriod,
        count: Int,
        fetchLimit: Int,
    ): MinuteWindow {
        if (minutesDescending.isEmpty()) return MinuteWindow(emptyList(), hasMoreBefore = false)
        val exhausted = minutesDescending.size < fetchLimit
        val buckets = minutesDescending
            .groupBy { it.date to bucketStart(it.time, period) }
            .map { (key, rows) -> merge(key.first, key.second, rows) }
            .sortedWith(compareByDescending(MinuteBucket::date).thenByDescending(MinuteBucket::time))
        return when {
            buckets.size > count -> MinuteWindow(buckets.take(count).asReversed(), hasMoreBefore = true)
            exhausted -> MinuteWindow(buckets.asReversed(), hasMoreBefore = false)
            else -> MinuteWindow(buckets.dropLast(1).asReversed(), hasMoreBefore = true)
        }
    }

    fun nextTo(window: MinuteWindow, period: MinutePeriod): String? {
        if (!window.hasMoreBefore) return null
        val oldest = window.buckets.firstOrNull() ?: return null
        return LocalDateTime.of(
            LocalDate.parse(oldest.date, DateTimeFormatter.BASIC_ISO_DATE),
            LocalTime.parse(oldest.time, HHMM),
        )
            .minusMinutes(1)
            .format(YYYYMMDDHHMM)
    }

    internal fun bucketStart(time: String, period: MinutePeriod): String {
        if (period == MinutePeriod.ONE) return time
        val minutes = minOf(time.take(2).toInt() * 60 + time.drop(2).toInt(), LAST_SESSION_MINUTE)
        val floored = OPEN_MINUTES + Math.floorDiv(minutes - OPEN_MINUTES, period.unitMinutes) * period.unitMinutes
        return "%02d%02d".format(floored / 60, floored % 60)
    }

    private fun merge(date: String, bucketTime: String, rows: List<MinuteCandleRow>): MinuteBucket {
        val ascending = rows.sortedBy(MinuteCandleRow::time)
        val first = ascending.first()
        val last = ascending.last()
        return MinuteBucket(
            date = date,
            time = bucketTime,
            open = first.open,
            high = ascending.maxOf(MinuteCandleRow::high),
            low = ascending.minOf(MinuteCandleRow::low),
            close = last.close,
            volume = ascending.sumOf(MinuteCandleRow::volume),
            value = ascending.sumOf(MinuteCandleRow::value),
        )
    }

    private const val OPEN_MINUTES = 9 * 60
    private const val LAST_SESSION_MINUTE = 15 * 60 + 29
    private const val MAX_FETCH_ROWS = 12_000
    private val HHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("HHmm")
    private val YYYYMMDDHHMM: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm")
}
