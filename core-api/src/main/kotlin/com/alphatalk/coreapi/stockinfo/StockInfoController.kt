package com.alphatalk.coreapi.stockinfo

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/v1/stocks")
class StockInfoController(
    private val stockInfo: StockInfoService,
) {
    @GetMapping("/{code}")
    fun overview(@PathVariable code: String): StockOverviewResponse = stockInfo.overview(code)

    @GetMapping("/{code}/candles")
    fun candles(
        @PathVariable code: String,
        @RequestParam(required = false) period: String?,
        @RequestParam(required = false) count: Int?,
        @RequestParam(required = false) to: String?,
    ): CandlesResponse = stockInfo.candles(code, period, count, to)

    @GetMapping("/{code}/valuation")
    fun valuation(@PathVariable code: String): ValuationResponse = stockInfo.valuation(code)

    @GetMapping("/{code}/financials")
    fun financials(
        @PathVariable code: String,
        @RequestParam(required = false) years: Int?,
    ): FinancialsResponse = stockInfo.financials(code, years)

    @GetMapping("/{code}/investors")
    fun investors(
        @PathVariable code: String,
        @RequestParam(required = false) days: Int?,
    ): InvestorsResponse = stockInfo.investors(code, days)
}
