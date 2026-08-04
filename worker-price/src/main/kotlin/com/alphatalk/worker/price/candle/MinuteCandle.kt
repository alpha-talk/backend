package com.alphatalk.worker.price.candle

data class MinuteCandle(
    val code: String,
    val date: String,
    val time: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)
