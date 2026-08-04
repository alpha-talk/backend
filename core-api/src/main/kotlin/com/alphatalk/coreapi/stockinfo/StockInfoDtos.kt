package com.alphatalk.coreapi.stockinfo

import com.fasterxml.jackson.annotation.JsonInclude

data class StockOverviewResponse(
    val code: String,
    val name: String,
    val market: String,
    val sector: String?,
    val sharesOutstanding: Long?,
    val listedAt: String?,
    val updatedAt: Long,
)

@JsonInclude(JsonInclude.Include.NON_NULL)
data class CandleView(
    val date: String,
    val time: String? = null,
    val open: Long,
    val high: Long,
    val low: Long,
    val close: Long,
    val volume: Long,
    val value: Long,
)

data class CandlePageInfo(
    val hasMoreBefore: Boolean,
    val nextTo: String?,
)

data class CandlesResponse(
    val period: String,
    val items: List<CandleView>,
    val pageInfo: CandlePageInfo,
)

data class ValuationResponse(
    val per: Double?,
    val pbr: Double?,
    val eps: Int?,
    val bps: Int?,
    val marketCap: Long?,
    val asOf: String,
)

data class FinancialRow(
    val period: String,
    val revenue: Long?,
    val operatingProfit: Long?,
    val netIncome: Long?,
    val assets: Long?,
    val liabilities: Long?,
    val equity: Long?,
    val source: String,
    val asOf: String,
)

data class FinancialsResponse(
    val annual: List<FinancialRow>,
    val quarterly: List<FinancialRow>,
)

data class InvestorFlowView(
    val date: String,
    val individual: Long,
    val foreign: Long,
    val institution: Long,
)

data class InvestorsResponse(val items: List<InvestorFlowView>)
