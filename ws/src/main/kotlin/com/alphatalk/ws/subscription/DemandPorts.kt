package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind

interface DemandQuery {
    fun usersWatching(code: String): Set<Long>
    fun roomHasQuoteViewers(code: String): Boolean
    fun isUserConnected(userId: Long): Boolean
    fun needsWatchlist(sessionId: String): Boolean
    fun connectedUserIds(): Set<Long>
    fun connectedSessionCount(): Int
}

interface DemandMutator {
    fun registerSession(sessionId: String, userId: Long)

    fun attachWatchlist(sessionId: String, watchlist: Set<String>)

    fun removeSession(sessionId: String)

    fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String)

    fun unsubscribeById(sessionId: String, subscriptionId: String)

    fun applyWatchlistDiff(userId: Long, added: Collection<String>, removed: Collection<String>)
}
