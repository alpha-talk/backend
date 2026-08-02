package com.alphatalk.coreapi.stockinfo

import java.time.Instant

data class FinancialRecord(
    val year: Int,
    val reprtCode: String,
    val fsDiv: String,
    val revenue: Long?,
    val operatingProfit: Long?,
    val netIncome: Long?,
    val assets: Long?,
    val liabilities: Long?,
    val equity: Long?,
    val disclosedAt: Instant,
)

interface FinancialsStore {
    fun findAll(code: String): List<FinancialRecord>
}
