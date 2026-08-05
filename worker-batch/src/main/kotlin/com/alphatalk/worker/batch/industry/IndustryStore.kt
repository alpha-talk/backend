package com.alphatalk.worker.batch.industry

data class StockIndustryRecord(
    val code: String,
    val indutyCode: String,
    val groupCode: String,
    val corpName: String?,
    val corpNameEng: String?,
    val stockName: String?,
    val homepage: String?,
)

interface IndustryStore {
    fun upsertIndustries(entries: List<KsicEntry>): Int

    fun upsertCorpMap(corps: List<DartCorp>): Int

    fun upsertStockIndustries(records: List<StockIndustryRecord>): Int

    fun activeStockCodes(): Set<String>
}
