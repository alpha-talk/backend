package com.alphatalk.worker.price.candle

interface MinuteRefreshWatermarkStore {
    fun fetchedThrough(code: String, date: String): String?
    fun record(code: String, date: String, time: String)
    fun clear(code: String, date: String)
}
