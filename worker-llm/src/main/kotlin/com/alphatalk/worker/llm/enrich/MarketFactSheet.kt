package com.alphatalk.worker.llm.enrich

import java.time.LocalDate

data class MarketFactSheet(
    val factDate: String,
    val advancers: Int,
    val decliners: Int,
    val unchanged: Int,
    val topSectors: List<SectorPerformance>,
    val bottomSectors: List<SectorPerformance>,
    val foreignNetBuyTop: List<SectorFlow>,
    val institutionNetBuyTop: List<SectorFlow>,
) {
    data class SectorPerformance(val name: String, val avgChangePct: Double, val stockCount: Int)

    data class SectorFlow(val name: String, val netBuy: Long)
}

sealed interface FactSheetLookup {
    data class Found(val sheet: MarketFactSheet) : FactSheetLookup
    data object Insufficient : FactSheetLookup
    data object Missing : FactSheetLookup
}

interface MarketFactSheetSource {
    fun lookup(onOrBefore: LocalDate): FactSheetLookup
}
