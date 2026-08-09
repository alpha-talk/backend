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

    fun completeBackfill(
        code: String,
        marker: BackfillMarker,
        rows: List<FinancialSummaryRow>,
        obsolete: List<ReportKey>,
        completedAt: Instant,
    )

    fun backfilledCodes(atLeast: BackfillMarker): Set<String>
}

data class BackfillMarker(
    val windowYears: Int,
    val mapperVersion: Int,
)

data class ReportKey(
    val year: Int,
    val reprtCode: String,
)

interface CorpDirectory {
    fun corpCodesFor(codes: Collection<String>): Map<String, String>
}
