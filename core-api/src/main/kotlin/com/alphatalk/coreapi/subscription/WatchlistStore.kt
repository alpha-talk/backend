package com.alphatalk.coreapi.subscription

data class WatchlistItem(
    val code: String,
    val name: String,
    val market: String,
    val subscribedAt: Long,
)

enum class SubscribeOutcome {
    ADDED,
    ALREADY_SUBSCRIBED,
    UNKNOWN_STOCK,
    LIMIT_EXCEEDED,
}

interface WatchlistStore {
    fun list(userId: Long): List<WatchlistItem>

    fun subscribe(userId: Long, code: String, limit: Int): SubscribeOutcome

    fun unsubscribe(userId: Long, code: String): Boolean
}
