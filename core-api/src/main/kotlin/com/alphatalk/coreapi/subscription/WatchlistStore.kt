package com.alphatalk.coreapi.subscription

data class WatchlistItem(
    val code: String,
    val name: String,
    val market: String,
    val subscribedAt: Long,
)

data class WatchlistState(
    val rev: Long,
    val codes: List<String>,
)

interface WatchlistStore {
    fun list(userId: Long): List<WatchlistItem>

    fun contains(userId: Long, code: String): Boolean

    fun count(userId: Long): Long

    fun add(userId: Long, code: String)

    fun remove(userId: Long, code: String): Boolean

    fun codes(userId: Long): List<String>

    fun nextRev(userId: Long): Long
}
