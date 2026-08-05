package com.alphatalk.worker.batch.industry

data class DartCorp(
    val corpCode: String,
    val stockCode: String?,
    val corpName: String,
)

data class DartCompany(
    val corpCode: String,
    val stockCode: String?,
    val indutyCode: String?,
    val corpName: String?,
    val corpNameEng: String?,
    val stockName: String?,
    val homepage: String?,
)

interface DartClient {
    fun corpCodes(): List<DartCorp>

    fun company(corpCode: String): DartCompany?
}
