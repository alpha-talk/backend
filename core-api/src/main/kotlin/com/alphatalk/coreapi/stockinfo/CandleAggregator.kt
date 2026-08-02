package com.alphatalk.coreapi.stockinfo

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.WeekFields
import java.util.Locale

enum class CandlePeriod(val token: String) {
    DAILY("D"),
    WEEKLY("W"),
    MONTHLY("M"),
    ;

    companion object {
        fun fromToken(token: String): CandlePeriod? =
            entries.firstOrNull { it.token == token.trim().uppercase() }
    }
}

data class CandleWindow(
    val candles: List<DailyCandle>,
    val hasMoreBefore: Boolean,
)

object CandleAggregator {
    fun fetchLimit(period: CandlePeriod, count: Int): Int = when (period) {
        CandlePeriod.DAILY -> count + 1
        CandlePeriod.WEEKLY -> count * MAX_TRADING_DAYS_PER_WEEK + 1
        CandlePeriod.MONTHLY -> count * MAX_TRADING_DAYS_PER_MONTH + 1
    }

    fun aggregate(dailyDescending: List<DailyCandle>, period: CandlePeriod, count: Int, fetchLimit: Int): CandleWindow {
        if (dailyDescending.isEmpty()) return CandleWindow(emptyList(), hasMoreBefore = false)
        if (period == CandlePeriod.DAILY) {
            val hasMore = dailyDescending.size > count
            return CandleWindow(dailyDescending.take(count).asReversed(), hasMore)
        }
        val exhausted = dailyDescending.size < fetchLimit
        val buckets = dailyDescending
            .groupBy { bucketKey(it.date, period) }
            .values
            .map(::merge)
            .sortedByDescending(DailyCandle::date)
        return when {
            buckets.size > count -> CandleWindow(buckets.take(count).asReversed(), hasMoreBefore = true)
            exhausted -> CandleWindow(buckets.asReversed(), hasMoreBefore = false)
            else -> CandleWindow(buckets.dropLast(1).asReversed(), hasMoreBefore = true)
        }
    }

    private fun bucketKey(date: String, period: CandlePeriod): String {
        val day = LocalDate.parse(date, DateTimeFormatter.BASIC_ISO_DATE)
        return when (period) {
            CandlePeriod.DAILY -> date
            CandlePeriod.WEEKLY -> {
                val week = WeekFields.ISO
                "%04d-W%02d".format(Locale.ROOT, day.get(week.weekBasedYear()), day.get(week.weekOfWeekBasedYear()))
            }

            CandlePeriod.MONTHLY -> "%04d-%02d".format(Locale.ROOT, day.year, day.monthValue)
        }
    }

    private fun merge(bucket: List<DailyCandle>): DailyCandle {
        val ascending = bucket.sortedBy(DailyCandle::date)
        val first = ascending.first()
        val last = ascending.last()
        return DailyCandle(
            date = last.date,
            open = first.open,
            high = ascending.maxOf(DailyCandle::high),
            low = ascending.minOf(DailyCandle::low),
            close = last.close,
            volume = ascending.sumOf(DailyCandle::volume),
            value = ascending.sumOf(DailyCandle::value),
        )
    }

    private const val MAX_TRADING_DAYS_PER_WEEK = 7
    private const val MAX_TRADING_DAYS_PER_MONTH = 31
}
