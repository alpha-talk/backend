package com.alphatalk.kis.rest

import java.math.BigDecimal

data class KisValuationSnapshot(
    val code: String,
    val price: Long,
    val per: BigDecimal?,
    val pbr: BigDecimal?,
    val eps: Int?,
    val bps: Int?,
)
