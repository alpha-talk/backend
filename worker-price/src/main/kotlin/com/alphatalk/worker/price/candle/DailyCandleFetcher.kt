package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import java.time.LocalDate

fun interface DailyCandleFetcher {
    fun fetch(code: String, from: LocalDate, to: LocalDate): List<KisDailyCandle>
}
