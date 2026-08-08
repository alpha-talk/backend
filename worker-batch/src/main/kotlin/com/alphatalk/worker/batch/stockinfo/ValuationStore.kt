package com.alphatalk.worker.batch.stockinfo

import java.math.BigDecimal

data class ValuationRow(
    val code: String,
    val date: String,
    val per: BigDecimal?,
    val pbr: BigDecimal?,
    val eps: Int?,
    val bps: Int?,
    val marketCap: Long?,
)

interface ValuationStore {
    fun upsert(rows: List<ValuationRow>): Int
}
