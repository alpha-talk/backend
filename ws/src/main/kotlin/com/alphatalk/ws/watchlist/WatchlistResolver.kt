package com.alphatalk.ws.watchlist

interface WatchlistResolver {
    fun resolve(userId: Long): Set<String>
}
