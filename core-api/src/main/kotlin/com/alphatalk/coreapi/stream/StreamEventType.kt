package com.alphatalk.coreapi.stream

import com.alphatalk.contracts.envelope.StreamCategory

enum class StreamEventType(val token: String, val storedType: String) {
    NEWS(StreamCategory.NEWS.payload, StreamCategory.NEWS.eventType),
    DISCLOSURE(StreamCategory.DISCLOSURE.payload, StreamCategory.DISCLOSURE.eventType),
    REPORT(StreamCategory.REPORT.payload, StreamCategory.REPORT.eventType),
    AI(StreamCategory.AI.payload, StreamCategory.AI.eventType),
    POST("post", "POST"),
    ALERT("alert", "PRICE_ALERT"),
    ;

    companion object {
        fun fromToken(token: String): StreamEventType? =
            entries.firstOrNull { it.token == token.trim().lowercase() }
    }
}
