package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.envelope.WatchlistUpdated

fun interface WatchlistResolver {
    fun resolve(userIds: List<Long>): List<WatchlistUpdated>
}
