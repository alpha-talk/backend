package com.alphatalk.worker.batch.industry

data class StockIndustryRecord(
    val code: String,
    val indutyCode: String,
    val sectorCode: String,
    val corpName: String?,
    val corpNameEng: String?,
    val stockName: String?,
    val homepage: String?,
)

interface IndustryStore {
    fun upsertSectors(entries: List<KsicEntry>): Int

    fun upsertCorpMap(corps: List<DartCorp>): Int

    fun upsertStockIndustries(records: List<StockIndustryRecord>): Int

    fun clearIndustryAssignments(codes: Collection<String>): Int

    fun activeStockCodes(): Set<String>

    fun activeIndutyCodes(): Map<String, String>
}
