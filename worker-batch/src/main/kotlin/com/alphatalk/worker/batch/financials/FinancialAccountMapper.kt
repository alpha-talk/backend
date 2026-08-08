package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartFinancialAccount

data class FinancialFigures(
    val revenue: Long?,
    val operatingProfit: Long?,
    val netIncome: Long?,
    val assets: Long?,
    val liabilities: Long?,
    val equity: Long?,
) {
    val isEmpty: Boolean
        get() = revenue == null && operatingProfit == null && netIncome == null &&
            assets == null && liabilities == null && equity == null
}

object FinancialAccountMapper {
    private const val KRW = "KRW"
    private val BALANCE_SHEET = setOf("BS")
    private val INCOME_STATEMENT = setOf("IS", "CIS")

    private val REVENUE_IDS = setOf("ifrs-full_Revenue", "ifrs_Revenue")
    private val REVENUE_NAMES = setOf("매출액", "수익(매출액)", "영업수익", "매출")
    private val OPERATING_IDS = setOf("dart_OperatingIncomeLoss")
    private val OPERATING_NAMES = setOf("영업이익", "영업이익(손실)", "영업손익")
    private val NET_INCOME_IDS = setOf("ifrs-full_ProfitLoss", "ifrs_ProfitLoss")
    private val NET_INCOME_NAMES = setOf(
        "당기순이익", "당기순이익(손실)", "당기순손익",
        "분기순이익", "분기순이익(손실)", "반기순이익", "반기순이익(손실)",
    )
    private val ASSET_IDS = setOf("ifrs-full_Assets", "ifrs_Assets")
    private val ASSET_NAMES = setOf("자산총계")
    private val LIABILITY_IDS = setOf("ifrs-full_Liabilities", "ifrs_Liabilities")
    private val LIABILITY_NAMES = setOf("부채총계")
    private val EQUITY_IDS = setOf("ifrs-full_Equity", "ifrs_Equity")
    private val EQUITY_NAMES = setOf("자본총계")

    fun map(accounts: List<DartFinancialAccount>): FinancialFigures {
        val topLevel = accounts.filter {
            it.accountDetail == null && (it.currency == null || it.currency.equals(KRW, ignoreCase = true))
        }
        return FinancialFigures(
            revenue = find(topLevel, INCOME_STATEMENT, REVENUE_IDS, REVENUE_NAMES),
            operatingProfit = find(topLevel, INCOME_STATEMENT, OPERATING_IDS, OPERATING_NAMES),
            netIncome = find(topLevel, INCOME_STATEMENT, NET_INCOME_IDS, NET_INCOME_NAMES),
            assets = find(topLevel, BALANCE_SHEET, ASSET_IDS, ASSET_NAMES),
            liabilities = find(topLevel, BALANCE_SHEET, LIABILITY_IDS, LIABILITY_NAMES),
            equity = find(topLevel, BALANCE_SHEET, EQUITY_IDS, EQUITY_NAMES),
        )
    }

    private fun find(
        accounts: List<DartFinancialAccount>,
        statements: Set<String>,
        ids: Set<String>,
        names: Set<String>,
    ): Long? {
        val scoped = accounts.filter { it.sjDiv in statements }
        return (scoped.firstOrNull { it.accountId in ids } ?: scoped.firstOrNull { it.accountName in names })
            ?.amount
    }
}
