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
    private val syncTrigger: DemandSyncTrigger,
) : DemandQuery, DemandMutator, DemandSnapshotSource {
    private class SessionInfo(
        val userId: Long,

        val roomSubs: MutableMap<String, RoomSub> = HashMap(),
    )

    private data class RoomSub(val kind: ChannelKind, val code: String)

    private class PendingDiff {
        val added = HashSet<String>()
        val removed = HashSet<String>()

        fun apply(add: Collection<String>, remove: Collection<String>) {
            added -= remove
            removed -= add
            added += add
            removed += remove
        }
    }

    private val lock = ReentrantLock()

    private val sessions = HashMap<String, SessionInfo>()
    private val userSessions = HashMap<Long, MutableSet<String>>()
    private val userWatchlists = HashMap<Long, MutableSet<String>>()
    private val pendingDiffs = HashMap<Long, PendingDiff>()
    private val roomIndex = HashMap<Pair<ChannelKind, String>, Int>()

    private val watchlistIndex = ConcurrentHashMap<String, Set<Long>>()
    private val roomQuoteCodes = ConcurrentHashMap.newKeySet<String>()

    override fun usersWatching(code: String): Set<Long> = watchlistIndex[code] ?: emptySet()

    override fun roomHasQuoteViewers(code: String): Boolean = code in roomQuoteCodes

    override fun demandSnapshot(): DemandSnapshot = lock.withLock {
        val quote = watchlistIndex.mapValues { it.value.size }
        val room = HashMap<String, Int>()
        roomIndex.forEach { (key, count) -> room.merge(key.second, count, Int::plus) }
        DemandSnapshot(quote = quote, room = room)
    }

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
            val merged = watchlist.toMutableSet()
            pendingDiffs.remove(info.userId)?.let { pending ->
                merged += pending.added
                merged -= pending.removed
            }
            userWatchlists[info.userId] = merged
            merged.forEach { addUserToCode(it, info.userId) }
        }
    }

    override fun removeSession(sessionId: String) {
        lock.withLock {
            val info = sessions.remove(sessionId) ?: return
            info.roomSubs.values.forEach { releaseRoom(it) }
            val remaining = userSessions[info.userId]?.apply { remove(sessionId) }
            if (remaining.isNullOrEmpty()) {
                userSessions.remove(info.userId)
                pendingDiffs.remove(info.userId)
                userWatchlists.remove(info.userId)?.forEach { removeUserFromCode(it, info.userId) }
            }
        }
    }

    override fun subscribeRoom(sessionId: String, subscriptionId: String, kind: ChannelKind, code: String) {
        require(
            kind == ChannelKind.QUOTE || kind == ChannelKind.POST ||
                kind == ChannelKind.TRADE || kind == ChannelKind.DEPTH,
        ) {
            "not a room channel kind: $kind"
        }
        lock.withLock {
            val info = sessions[sessionId] ?: return
            val previous = info.roomSubs.put(subscriptionId, RoomSub(kind, code))
            if (previous != null) releaseRoom(previous)
            val count = roomIndex.merge(kind to code, 1, Int::plus)
            if (count == 1) {
                if (kind == ChannelKind.QUOTE) {
                    roomQuoteCodes.add(code)
                    if (!watchlistIndex.containsKey(code)) {
                        channelSubscriber.subscribe(Channels.of(kind, code))
                    }
                } else {
                    channelSubscriber.subscribe(Channels.of(kind, code))
                }
                syncTrigger.request()
            }
        }
    }

    override fun unsubscribeById(sessionId: String, subscriptionId: String) {
        lock.withLock {
            val info = sessions[sessionId] ?: return
            val sub = info.roomSubs.remove(subscriptionId) ?: return
            releaseRoom(sub)
        }
    }

    override fun applyWatchlistDiff(userId: Long, added: Collection<String>, removed: Collection<String>) {
        lock.withLock {
            if (userId !in userSessions) return
            val watchlist = userWatchlists[userId] ?: run {
                pendingDiffs.getOrPut(userId) { PendingDiff() }.apply(added, removed)
                return
            }
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
            if (code !in roomQuoteCodes) channelSubscriber.subscribe(Channels.quote(code))
            channelSubscriber.subscribe(Channels.stream(code))
            syncTrigger.request()
        }
    }

    private fun removeUserFromCode(code: String, userId: Long) {
        val before = watchlistIndex[code] ?: return
        if (userId !in before) return
        val after = before - userId
        if (after.isEmpty()) {
            watchlistIndex.remove(code)
            if (code !in roomQuoteCodes) channelSubscriber.unsubscribe(Channels.quote(code))
            channelSubscriber.unsubscribe(Channels.stream(code))
            syncTrigger.request()
        } else {
            watchlistIndex[code] = after
        }
    }

    private fun releaseRoom(sub: RoomSub) {
        val key = sub.kind to sub.code
        val count = roomIndex[key] ?: return
        if (count <= 1) {
            roomIndex.remove(key)
            if (sub.kind == ChannelKind.QUOTE) {
                roomQuoteCodes.remove(sub.code)
                if (!watchlistIndex.containsKey(sub.code)) {
                    channelSubscriber.unsubscribe(Channels.of(sub.kind, sub.code))
                }
            } else {
                channelSubscriber.unsubscribe(Channels.of(sub.kind, sub.code))
            }
            syncTrigger.request()
        } else {
            roomIndex[key] = count - 1
        }
    }
}
