package com.alphatalk.kis.rest

data class KisDailyCandle(
    val code: String,
    val date: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)
