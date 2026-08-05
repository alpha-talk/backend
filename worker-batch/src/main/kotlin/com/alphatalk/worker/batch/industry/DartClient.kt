package com.alphatalk.worker.batch.industry

data class DartCorp(
    val corpCode: String,
    val stockCode: String?,
    val corpName: String,
    val modifyDate: String?,
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

class DartApiException(val status: String, message: String) : RuntimeException(message)

interface DartClient {
    fun corpCodes(): List<DartCorp>

    fun company(corpCode: String): DartCompany?
}
