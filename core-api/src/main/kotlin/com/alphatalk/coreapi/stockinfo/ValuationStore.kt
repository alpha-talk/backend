package com.alphatalk.coreapi.stockinfo

import java.math.BigDecimal

data class ValuationRecord(
    val date: String,
    val per: BigDecimal?,
    val pbr: BigDecimal?,
    val eps: Int?,
    val bps: Int?,
    val marketCapWon: Long?,
)

interface ValuationStore {
    fun findLatest(code: String): ValuationRecord?
}
