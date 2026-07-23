package com.alphatalk.contracts.envelope

enum class StreamCategory(val payload: String, val eventType: String) {
    NEWS("news", "NEWS"),
    REPORT("report", "REPORT"),
    DISCLOSURE("disclosure", "DISCLOSURE"),
    AI("ai", "AI"),
    ;

    companion object {
        fun fromPayload(value: String): StreamCategory? = entries.firstOrNull { it.payload == value }
    }
}
