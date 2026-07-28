package com.alphatalk.kis.master

import java.time.LocalDate

data class KisStockMaster(
    val code: String,
    val name: String,
    val market: KisMarket,
    val groupCode: String,
    val sectorCode: String?,
    val sharesOutstanding: Long?,
    val listedAt: LocalDate?,
) {
    val isCommonStock: Boolean
        get() = groupCode == GROUP_COMMON_STOCK

    companion object {
        const val GROUP_COMMON_STOCK = "ST"
    }
}
