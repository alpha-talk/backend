package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteCandle
import java.time.LocalDate
import java.time.LocalTime

fun interface DailyMinuteCandleFetcher {
    fun fetch(code: String, date: LocalDate, to: LocalTime, marketDiv: String): List<KisMinuteCandle>
}
