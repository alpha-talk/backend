package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.envelope.WatchlistUpdated
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

@Component
class HttpWatchlistResolver(
    private val client: WatchlistSnapshotClient,
    @param:Value("\${ALPHATALK_WATCHLIST_INTERNAL_KEY:}") private val key: String,
) : WatchlistResolver {
    override fun resolve(userIds: List<Long>): List<WatchlistUpdated> {
        check(key.isNotBlank()) { "Watchlist internal key is missing" }
        return requireNotNull(client.snapshots(key, WatchlistSnapshotRequest(userIds)))
    }
}
