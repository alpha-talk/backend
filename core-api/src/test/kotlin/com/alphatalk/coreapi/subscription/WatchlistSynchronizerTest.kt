package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.auth.UserAccountLock
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class WatchlistSynchronizerTest {
    private class FakeStore : WatchlistStore {
        var subscribed = true

        override fun list(userId: Long): List<WatchlistItem> = emptyList()
        override fun subscribe(userId: Long, code: String, limit: Int) = SubscribeOutcome.ADDED
        override fun unsubscribe(userId: Long, code: String) = UnsubscribeOutcome.REMOVED
        override fun contains(userId: Long, code: String): Boolean = subscribed
    }

    private class FakeOwnerLock(var exists: Boolean = true) : UserAccountLock {
        override fun acquire(userId: Long): Boolean = exists
    }

    private class RecordingBroadcaster : WatchlistMirror, WatchlistAnnouncer {
        data class Announcement(val userId: Long, val added: List<String>, val removed: List<String>)

        val mirrored = linkedSetOf<Pair<Long, String>>()
        val announcements = mutableListOf<Announcement>()

        override fun add(userId: Long, code: String) {
            mirrored += userId to code
        }

        override fun remove(userId: Long, code: String) {
            mirrored -= userId to code
        }

        override fun announce(userId: Long, added: List<String>, removed: List<String>) {
            announcements += Announcement(userId, added, removed)
        }
    }

    private val store = FakeStore()
    private val owners = FakeOwnerLock()
    private val broadcaster = RecordingBroadcaster()
    private val synchronizer = TransactionalWatchlistSynchronizer(store, owners, broadcaster, broadcaster)

    @Test
    fun `현재 DB에 있으면 미러와 게이트웨이에 added를 적용한다`() {
        synchronizer.synchronize(1L, "005930")

        assertEquals(setOf(1L to "005930"), broadcaster.mirrored)
        assertEquals(
            RecordingBroadcaster.Announcement(1L, listOf("005930"), emptyList()),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `현재 DB에 없으면 미러와 게이트웨이에 removed를 적용한다`() {
        broadcaster.mirrored += 1L to "005930"
        store.subscribed = false

        synchronizer.synchronize(1L, "005930")

        assertTrue(broadcaster.mirrored.isEmpty())
        assertEquals(
            RecordingBroadcaster.Announcement(1L, emptyList(), listOf("005930")),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `사용자가 사라지면 Redis를 건드리지 않고 401이다`() {
        owners.exists = false

        val failure = assertFailsWith<ApiException> {
            synchronizer.synchronize(1L, "005930")
        }

        assertEquals(ErrorCode.UNAUTHORIZED, failure.code)
        assertTrue(broadcaster.mirrored.isEmpty())
        assertTrue(broadcaster.announcements.isEmpty())
    }
}
