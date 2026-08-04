package com.alphatalk.worker.price.candle

fun interface CandleUniverse {
    fun symbols(): Set<String>
}
