package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.auth.UserAccountLock
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

interface WatchlistSynchronizer {
    fun synchronize(userId: Long, code: String)
}

@Service
class TransactionalWatchlistSynchronizer(
    private val store: WatchlistStore,
    private val owners: UserAccountLock,
    private val mirror: WatchlistMirror,
    private val announcer: WatchlistAnnouncer,
) : WatchlistSynchronizer {
    @Transactional
    override fun synchronize(userId: Long, code: String) {
        if (!owners.acquire(userId)) throw unauthorized()
        if (store.contains(userId, code)) {
            mirror.add(userId, code)
            announcer.announce(userId, added = listOf(code), removed = emptyList())
        } else {
            mirror.remove(userId, code)
            announcer.announce(userId, added = emptyList(), removed = listOf(code))
        }
    }

    private fun unauthorized() =
        ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")
}
