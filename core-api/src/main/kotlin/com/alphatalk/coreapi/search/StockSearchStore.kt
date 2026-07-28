package com.alphatalk.coreapi.search

data class StockSummary(
    val code: String,
    val name: String,
    val market: String,
)

interface StockSearchStore {
    fun search(query: String, limit: Int): List<StockSummary>
}
