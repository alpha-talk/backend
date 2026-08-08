package com.alphatalk.worker.batch.stockinfo

data class ActiveStock(
    val code: String,
    val sharesOutstanding: Long?,
)

interface StockUniverse {
    fun activeStocks(): List<ActiveStock>

    fun activeCodes(): Set<String>
}
