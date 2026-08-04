package com.alphatalk.worker.price.candle

fun interface MinuteRefreshUniverse {
    fun contains(code: String): Boolean
}
