package com.alphatalk.worker.price.candle

interface MinuteMarketDivStore {
    fun get(code: String): String?
    fun put(code: String, div: String)
}
