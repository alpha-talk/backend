package com.alphatalk.contracts

object WatchlistSnapshots {
    const val PATH = "/internal/watchlists/snapshots"
    const val KEY_HEADER = "X-AlphaTalk-Watchlist-Key"
    const val MAX_BATCH_SIZE = 100
}

data class WatchlistSnapshotRequest(val userIds: List<Long?>)
