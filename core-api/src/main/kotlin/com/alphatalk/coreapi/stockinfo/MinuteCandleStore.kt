package com.alphatalk.coreapi.stockinfo

data class MinuteCandleRow(
    val date: String,
    val time: String,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)

interface MinuteCandleStore {
    fun findLatestUpTo(code: String, toDate: String?, toTime: String?, limit: Int): List<MinuteCandleRow>
}

fun interface MinuteCandleRefresher {
    fun refresh(code: String)
}
