package com.alphatalk.coreapi.stockinfo

data class DailyCandle(
    val date: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)

interface CandleStore {
    fun findLatestUpTo(code: String, toDate: String?, limit: Int): List<DailyCandle>
}
