package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle

interface DailyCandleStore {
    fun upsert(candles: List<KisDailyCandle>): Int
    fun latestDate(code: String): String?
}
