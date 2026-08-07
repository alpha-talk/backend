package com.alphatalk.contracts

object Keys {
    fun presence(userId: Long) = "presence:$userId"
    fun price(code: String) = "price:$code"
    fun cursor(userId: Long, code: String) = "cursor:$userId:$code"

    fun watchlist(userId: Long) = "watchlist:$userId"

    fun watchlistRev(userId: Long) = "watchlist:rev:$userId"

    fun seenIngest(sourceId: String) = "seen:ingest:$sourceId"
    fun clusterLock(code: String) = "lock:cluster:$code"
    fun articleFetchRate(host: String) = "rate:article-fetch:$host"

    const val GW_ALIVE_PREFIX = "gw:alive:"

    fun demandQuote(gwId: String) = "demand:quote:$gwId"
    fun demandRoom(gwId: String) = "demand:room:$gwId"
    fun gwAlive(gwId: String) = "$GW_ALIVE_PREFIX$gwId"

    fun rateLimitWindow(action: String, key: String, windowIndex: Long) = "rl:$action:$key:$windowIndex"

    fun minuteRefreshLock(code: String) = "lock:minute-refresh:$code"

    fun kisRestRate(keyId: String) = "rate:kis-rest:$keyId"

    fun minuteRefreshWatermark(code: String, date: String) = "minute:through:$code:$date"

    fun marketDiv(code: String) = "market-div:$code"

    fun minuteMarketDiv(code: String, date: String) = "minute:market-div:$code:$date"

    fun badge(userId: Long) = "badge:$userId"
}
