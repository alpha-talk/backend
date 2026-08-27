package com.alphatalk.worker.llm.sector

data class SectorInfo(val code: String, val name: String)

interface SectorDirectory {
    fun allSectors(): List<SectorInfo>
    fun sectorName(sectorCode: String): String?
    fun memberCodes(sectorCode: String): List<String>
    fun stockName(stockCode: String): String?
    fun stockAliases(stockCode: String): Set<String> = setOfNotNull(stockName(stockCode))
    fun sectorOf(stockCode: String): String?
}
