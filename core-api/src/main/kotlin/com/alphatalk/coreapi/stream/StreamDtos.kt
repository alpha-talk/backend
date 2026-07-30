package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.JsonNode

enum class CursorDirection(val token: String) {
    BEFORE("before"),
    AFTER("after"),
    ;

    companion object {
        fun fromToken(token: String): CursorDirection? =
            entries.firstOrNull { it.token == token.trim().lowercase() }
    }
}

data class StreamItem(
    val eventId: String,
    val code: String,
    val type: String,
    val occurredAt: Long,
    val source: String?,
    val payload: JsonNode,
)

data class PageInfo(
    val oldest: String?,
    val newest: String?,
    val hasMoreBefore: Boolean,
    val hasMoreAfter: Boolean,
)

data class StreamPage(
    val items: List<StreamItem>,
    val pageInfo: PageInfo,
)

data class StreamQuery(
    val code: String,
    val cursor: String?,
    val direction: CursorDirection,
    val limit: Int,
    val types: List<StreamEventType>,
)

data class QuoteResponse(
    val code: String,
    val price: Long,
    val prevClose: Long,
    val change: Long,
    val changeRate: Double,
    val open: Long,
    val high: Long,
    val low: Long,
    val volume: Long,
    val ts: Long,
    val delayed: Boolean,
)
