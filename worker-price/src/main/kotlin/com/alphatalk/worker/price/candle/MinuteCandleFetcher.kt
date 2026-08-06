package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisMinuteChart
import java.time.LocalTime

fun interface MinuteCandleFetcher {
    fun fetch(code: String, to: LocalTime, marketDiv: String): KisMinuteChart
}
