package com.alphatalk.worker.price.candle

interface MinuteMarketDivStore {
    fun get(code: String, date: String): String?
    fun put(code: String, date: String, div: String)
}
