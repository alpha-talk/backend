package com.alphatalk.kis.ws

data class KisTick(
    val code: String,
    val time: String,
    val price: Long,
    val change: Long,
    val changeRate: Double,
    val open: Long,
    val high: Long,
    val low: Long,
    val volume: Long,
)
