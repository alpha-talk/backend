package com.alphatalk.worker.batch.industry

import java.time.LocalDate

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

data class DartDisclosure(
    val corpCode: String,
    val stockCode: String?,
    val reportName: String,
    val receiptDate: String,
)

data class DartFinancialAccount(
    val sjDiv: String,
    val accountId: String?,
    val accountName: String,
    val accountDetail: String?,
    val amount: Long?,
    val currency: String? = null,
)

class DartApiException(val status: String, message: String) : RuntimeException(message)

interface DartClient {
    fun corpCodes(): List<DartCorp>

    fun company(corpCode: String): DartCompany?

    fun periodicDisclosures(begin: LocalDate, end: LocalDate, corpCode: String? = null): List<DartDisclosure>

    fun financialAccounts(corpCode: String, year: Int, reprtCode: String, fsDiv: String): List<DartFinancialAccount>
}
