package com.alphatalk.coreapi.search

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode

class StockSearchService(
    private val store: StockSearchStore,
) {
    fun search(rawQuery: String?, rawLimit: Int?): List<StockSummary> {
        val query = rawQuery?.trim().orEmpty()
        if (query.isEmpty()) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "검색어는 1자 이상이어야 합니다", mapOf("field" to "q"))
        }
        val limit = rawLimit ?: DEFAULT_LIMIT
        if (limit < MIN_LIMIT || limit > MAX_LIMIT) {
            throw ApiException(
                ErrorCode.VALIDATION_FAILED,
                "limit은 $MIN_LIMIT~$MAX_LIMIT 사이여야 합니다",
                mapOf("field" to "limit"),
            )
        }
        return store.search(query, limit)
    }

    companion object {
        const val DEFAULT_LIMIT = 10
        const val MIN_LIMIT = 1
        const val MAX_LIMIT = 30
    }
}
