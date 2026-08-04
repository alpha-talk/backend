package com.alphatalk.worker.price.candle

interface MinuteCandleStore {
    fun upsert(candles: List<MinuteCandle>): Int
    fun latestTime(code: String, date: String): String?
    fun sumValueBefore(code: String, date: String, timeExclusive: String): Long
    fun codesOn(date: String): Set<String>
    fun purgeBefore(dateExclusive: String): Int
}
