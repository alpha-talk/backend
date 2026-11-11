package com.alphatalk.ws.watchlist

import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.contracts.envelope.WatchlistUpdated
import org.springframework.cloud.openfeign.FeignClient
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader

@FeignClient(name = "watchlist-snapshots", url = "\${ALPHATALK_WATCHLIST_CORE_API_URL:http://localhost:8080}")
interface WatchlistSnapshotClient {
    @PostMapping(path = [WatchlistSnapshots.PATH], consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun snapshots(
        @RequestHeader(WatchlistSnapshots.KEY_HEADER) key: String,
        @RequestBody request: WatchlistSnapshotRequest,
    ): List<WatchlistUpdated>?
}
