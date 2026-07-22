package com.alphatalk.contracts

object Keys {
    fun presence(userId: Long) = "presence:$userId"
    fun price(code: String) = "price:$code"
    fun cursor(userId: Long, code: String) = "cursor:$userId:$code"

    fun watchlist(userId: Long) = "watchlist:$userId"

    fun seenIngest(sourceId: String) = "seen:ingest:$sourceId"
    fun clusterLock(code: String) = "lock:cluster:$code"
}
