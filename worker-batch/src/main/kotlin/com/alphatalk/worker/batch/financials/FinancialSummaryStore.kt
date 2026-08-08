package com.alphatalk.worker.batch.financials

import java.time.Instant

data class FinancialSummaryRow(
    val code: String,
    val year: Int,
    val reprtCode: String,
    val fsDiv: String,
    val figures: FinancialFigures,
    val disclosedAt: Instant,
)

interface FinancialSummaryStore {
    fun upsert(row: FinancialSummaryRow)
}
