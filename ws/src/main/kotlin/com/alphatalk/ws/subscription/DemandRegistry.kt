package com.alphatalk.ws.subscription

import com.alphatalk.contracts.ChannelKind
import com.alphatalk.contracts.Channels
import com.alphatalk.ws.relay.ChannelSubscriber
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

@Component
class DemandRegistry(
    private val channelSubscriber: ChannelSubscriber,
) : DemandQuery, DemandMutator {
    private class SessionInfo(
        val userId: Long,

        val roomSubs: MutableMap<String, RoomSub> = HashMap(),
    )

    private data class RoomSub(val kind: ChannelKind, val code: String)

    private val lock = ReentrantLock()

    private val sessions = HashMap<String, SessionInfo>()
    private val userSessions = HashMap<Long, MutableSet<String>>()
    private val userWatchlists = HashMap<Long, MutableSet<String>>()
    private val roomIndex = HashMap<Pair<ChannelKind, String>, MutableSet<String>>()

    private val watchlistIndex = ConcurrentHashMap<String, Set<Long>>()

    override fun usersWatching(code: String): Set<Long> = watchlistIndex[code] ?: emptySet()

    override fun isUserConnected(userId: Long): Boolean = lock.withLock { userId in userSessions }

    override fun needsWatchlist(sessionId: String): Boolean = lock.withLock {
        val info = sessions[sessionId] ?: return false
        info.userId !in userWatchlists
    }

    override fun connectedUserIds(): Set<Long> = lock.withLock { userSessions.keys.toSet() }

    override fun connectedSessionCount(): Int = lock.withLock { sessions.size }

    override fun registerSession(sessionId: String, userId: Long) {
        lock.withLock {
            if (sessionId in sessions) return
            sessions[sessionId] = SessionInfo(userId)
            userSessions.getOrPut(userId) { HashSet() }.add(sessionId)
        }
    }

    override fun attachWatchlist(sessionId: String, watchlist: Set<String>) {
        lock.withLock {
            val info = sessions[sessionId] ?: return
            if (info.userId in userWatchlists) return
            userWatchlists[info.userId] = watchlist.toMutableSet()
            watchlist.forEach { addUserToCode(it, info.userId) }
        }
    }

    override fun removeSession(sessionId: String) {
        lock.withLock {
            val info = sessions.remove(sessionId) ?: return
            info.roomSubs.values.forEach { releaseRoom(it, sessionId) }
            val remaining = userSessions[info.userId]?.apply { remove(sessionId) }
            if (remaining.isNullOrEmpty()) {
                userSessions.remove(info.userId)
                userWatchlists.remove(info.userId)?.forEach { removeUserFromCode(it, info.userId) }
            }
        }
    }

    override fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String) {
        require(kind == ChannelKind.POST || kind == ChannelKind.TRADE || kind == ChannelKind.DEPTH) {
            "not a room channel kind: $kind"
        }
        lock.withLock {
            val info = sessions[sessionId] ?: return
            val previous = info.roomSubs.put(subscriptionId, RoomSub(kind, code))
            if (previous != null) releaseRoom(previous, sessionId)
            val members = roomIndex.getOrPut(kind to code) { HashSet() }
            if (members.add(sessionId) && members.size == 1) {
                channelSubscriber.subscribe(Channels.of(kind, code))
            }
        }
    }

    override fun unsubscribeById(sessionId: String, subscriptionId: String) {
        lock.withLock {
            val info = sessions[sessionId] ?: return
            val sub = info.roomSubs.remove(subscriptionId) ?: return
            releaseRoom(sub, sessionId)
        }
    }

    override fun applyWatchlistDiff(userId: Long, added: Collection<String>, removed: Collection<String>) {
        lock.withLock {
            if (userId !in userSessions) return
            val watchlist = userWatchlists[userId] ?: return
            added.forEach { code ->
                if (watchlist.add(code)) addUserToCode(code, userId)
            }
            removed.forEach { code ->
                if (watchlist.remove(code)) removeUserFromCode(code, userId)
            }
        }
    }

    private fun addUserToCode(code: String, userId: Long) {
        val before = watchlistIndex[code] ?: emptySet()
        if (userId in before) return
        watchlistIndex[code] = before + userId
        if (before.isEmpty()) {
            channelSubscriber.subscribe(Channels.quote(code))
            channelSubscriber.subscribe(Channels.stream(code))
        }
    }

    private fun removeUserFromCode(code: String, userId: Long) {
        val before = watchlistIndex[code] ?: return
        if (userId !in before) return
        val after = before - userId
        if (after.isEmpty()) {
            watchlistIndex.remove(code)
            channelSubscriber.unsubscribe(Channels.quote(code))
            channelSubscriber.unsubscribe(Channels.stream(code))
        } else {
            watchlistIndex[code] = after
        }
    }

    private fun releaseRoom(sub: RoomSub, sessionId: String) {
        val key = sub.kind to sub.code
        val members = roomIndex[key] ?: return
        if (members.remove(sessionId) && members.isEmpty()) {
            roomIndex.remove(key)
            channelSubscriber.unsubscribe(Channels.of(sub.kind, sub.code))
        }
    }
}
