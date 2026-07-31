package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.auth.UserAccountLock
import com.alphatalk.coreapi.search.StockCatalog
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

enum class SubscribeOutcome {
    ADDED,
    ALREADY_SUBSCRIBED,
    UNKNOWN_STOCK,
    LIMIT_EXCEEDED,
    OWNER_MISSING,
}

enum class UnsubscribeOutcome {
    REMOVED,
    ALREADY_REMOVED,
    OWNER_MISSING,
}

data class SubscribeChange(
    val outcome: SubscribeOutcome,
    val state: WatchlistState? = null,
)

data class UnsubscribeChange(
    val outcome: UnsubscribeOutcome,
    val state: WatchlistState? = null,
)

interface WatchlistCommand {
    fun subscribe(userId: Long, code: String, limit: Int): SubscribeChange

    fun unsubscribe(userId: Long, code: String): UnsubscribeChange
}

@Service
class TransactionalWatchlistCommand(
    private val store: WatchlistStore,
    private val stocks: StockCatalog,
    private val owners: UserAccountLock,
) : WatchlistCommand {
    @Transactional
    override fun subscribe(userId: Long, code: String, limit: Int): SubscribeChange {
        if (!owners.acquire(userId)) return SubscribeChange(SubscribeOutcome.OWNER_MISSING)
        if (store.contains(userId, code)) {
            return SubscribeChange(SubscribeOutcome.ALREADY_SUBSCRIBED, nextState(userId))
        }
        if (!stocks.existsActive(code)) return SubscribeChange(SubscribeOutcome.UNKNOWN_STOCK)
        if (store.count(userId) >= limit) return SubscribeChange(SubscribeOutcome.LIMIT_EXCEEDED)
        store.add(userId, code)
        return SubscribeChange(SubscribeOutcome.ADDED, nextState(userId))
    }

    @Transactional
    override fun unsubscribe(userId: Long, code: String): UnsubscribeChange {
        if (!owners.acquire(userId)) return UnsubscribeChange(UnsubscribeOutcome.OWNER_MISSING)
        val outcome = if (store.remove(userId, code)) {
            UnsubscribeOutcome.REMOVED
        } else {
            UnsubscribeOutcome.ALREADY_REMOVED
        }
        return UnsubscribeChange(outcome, nextState(userId))
    }

    private fun nextState(userId: Long) = WatchlistState(store.nextRev(userId), store.codes(userId))
}
