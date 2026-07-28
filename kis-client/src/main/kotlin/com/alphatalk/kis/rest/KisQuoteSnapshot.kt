package com.alphatalk.kis.rest

data class KisQuoteSnapshot(
    val code: String,
    val price: Long,
    val change: Long,
    val changeRate: Double,
    val open: Long,
    val high: Long,
    val low: Long,
    val volume: Long,
)
