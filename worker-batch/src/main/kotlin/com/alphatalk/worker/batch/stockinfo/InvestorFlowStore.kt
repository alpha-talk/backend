package com.alphatalk.worker.batch.stockinfo

data class InvestorFlowRow(
    val code: String,
    val date: String,
    val individual: Long,
    val foreign: Long,
    val institution: Long,
)

interface InvestorFlowStore {
    fun upsert(rows: List<InvestorFlowRow>): Int
}
