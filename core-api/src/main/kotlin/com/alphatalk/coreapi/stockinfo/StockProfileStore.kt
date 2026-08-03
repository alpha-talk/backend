package com.alphatalk.coreapi.stockinfo

import java.time.Instant
import java.time.LocalDate

data class StockProfile(
    val code: String,
    val name: String,
    val market: String,
    val sectorName: String?,
    val sharesOutstanding: Long?,
    val listedAt: LocalDate?,
    val updatedAt: Instant,
)

interface StockProfileStore {
    fun findActive(code: String): StockProfile?

    fun existsActive(code: String): Boolean
}
