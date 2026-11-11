package com.alphatalk.coreapi.subscription

import com.alphatalk.contracts.WatchlistSnapshotRequest
import com.alphatalk.contracts.WatchlistSnapshots
import com.alphatalk.contracts.envelope.WatchlistUpdated
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RestController
import java.security.MessageDigest

@RestController
class InternalWatchlistController(
    private val reader: WatchlistSnapshotReader,
    @param:Value("\${ALPHATALK_WATCHLIST_INTERNAL_KEY:}") private val key: String,
) {
    @PostMapping(WatchlistSnapshots.PATH)
    fun snapshots(
        @RequestHeader(WatchlistSnapshots.KEY_HEADER, defaultValue = "") suppliedKey: String,
        @RequestBody request: WatchlistSnapshotRequest,
    ): List<WatchlistUpdated> {
        if (key.isBlank() || !MessageDigest.isEqual(key.toByteArray(), suppliedKey.toByteArray())) {
            throw ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")
        }
        val userIds = request.userIds.filterNotNull()
        if (userIds.size != request.userIds.size ||
            userIds.size !in 1..WatchlistSnapshots.MAX_BATCH_SIZE || userIds.any { it <= 0 }
        ) {
            throw ApiException(ErrorCode.VALIDATION_FAILED, "사용자 ID 요청 범위가 올바르지 않습니다")
        }
        return reader.read(userIds.distinct())
    }
}
