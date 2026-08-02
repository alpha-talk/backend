package com.alphatalk.coreapi.stockinfo

data class InvestorFlowRecord(
    val date: String,
    val individual: Long,
    val foreign: Long,
    val institution: Long,
)

interface InvestorFlowStore {
    fun findLatest(code: String, days: Int): List<InvestorFlowRecord>
}
