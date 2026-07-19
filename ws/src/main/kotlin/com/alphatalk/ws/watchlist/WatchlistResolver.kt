package com.alphatalk.ws.watchlist

fun interface WatchlistResolver {
    fun resolve(userId: Long): Set<String>
}
