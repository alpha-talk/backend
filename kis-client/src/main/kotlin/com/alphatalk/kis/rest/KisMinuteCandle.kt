package com.alphatalk.kis.rest

data class KisMinuteChart(
    val dailyVolume: Long,
    val candles: List<KisMinuteCandle>,
)

data class KisMinuteCandle(
    val code: String,
    val date: String,
    val time: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val accValue: Long,
)
