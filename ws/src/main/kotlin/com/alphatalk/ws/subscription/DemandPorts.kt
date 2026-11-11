package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.envelope.WatchlistUpdated

data class WatchlistTarget(val userId: Long, val generation: String)

interface DemandQuery {
    fun usersWatching(code: String): Set<Long>
    fun roomHasQuoteViewers(code: String): Boolean
    fun isUserConnected(userId: Long): Boolean
    fun watchlistTargets(): List<WatchlistTarget>
    fun connectedUserIds(): Set<Long>
    fun connectedSessionCount(): Int
}

interface DemandMutator {
    fun registerSession(sessionId: String, userId: Long)

    fun startWatchlist(sessionId: String): WatchlistTarget?

    fun removeSession(sessionId: String)

    fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String)

    fun unsubscribeById(sessionId: String, subscriptionId: String)

    fun applyWatchlist(snapshot: WatchlistUpdated, target: WatchlistTarget? = null)
}
