package com.alphatalk.coreapi.search

import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController

data class StockSearchResponse(val items: List<StockSummary>)

@RestController
@RequestMapping("/api/v1/stocks")
class StockSearchController(
    private val search: StockSearchService,
) {
    @GetMapping("/search")
    fun search(
        @RequestParam(required = false) q: String?,
        @RequestParam(required = false) limit: Int?,
    ): StockSearchResponse = StockSearchResponse(search.search(q, limit))
}
