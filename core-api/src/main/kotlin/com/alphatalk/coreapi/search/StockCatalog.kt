package com.alphatalk.coreapi.search

data class StockRef(
    val code: String,
    val name: String,
    val market: String,
)

interface StockCatalog {
    fun existsActive(code: String): Boolean

    fun refs(codes: Collection<String>): Map<String, StockRef>
}
