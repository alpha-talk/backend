package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class WatchlistServiceTest {
    private class FakeStore : WatchlistStore {
        var outcome = SubscribeOutcome.ADDED
        var removes = true
        val subscribeCalls = mutableListOf<Triple<Long, String, Int>>()

        override fun list(userId: Long): List<WatchlistItem> = emptyList()

        override fun subscribe(userId: Long, code: String, limit: Int): SubscribeOutcome {
            subscribeCalls += Triple(userId, code, limit)
            return outcome
        }

        override fun unsubscribe(userId: Long, code: String): Boolean = removes
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
    private val broadcaster = RecordingBroadcaster()
    private val service = WatchlistService(store, broadcaster, broadcaster)

    @Test
    fun `새 종목을 담으면 미러에 넣고 게이트웨이에 알린다`() {
        val created = service.subscribe(1L, "005930")

        assertTrue(created)
        assertEquals(setOf(1L to "005930"), broadcaster.mirrored)
        assertEquals(
            RecordingBroadcaster.Announcement(1L, listOf("005930"), emptyList()),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `한도는 서비스가 정해 저장소에 넘긴다`() {
        service.subscribe(1L, "005930")

        assertEquals(Triple(1L, "005930", WatchlistService.MAX_ITEMS), store.subscribeCalls.single())
    }

    @Test
    fun `이미 담긴 종목 재요청도 게이트웨이에 다시 알린다`() {
        store.outcome = SubscribeOutcome.ALREADY_SUBSCRIBED

        val created = service.subscribe(1L, "005930")

        assertFalse(created)
        assertEquals(
            RecordingBroadcaster.Announcement(1L, listOf("005930"), emptyList()),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `재요청은 어긋난 미러를 되돌려 놓는다`() {
        store.outcome = SubscribeOutcome.ALREADY_SUBSCRIBED

        service.subscribe(1L, "005930")

        assertEquals(setOf(1L to "005930"), broadcaster.mirrored, "미러가 복구되지 않았다")
        assertEquals(
            RecordingBroadcaster.Announcement(1L, listOf("005930"), emptyList()),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `없는 종목은 404다`() {
        store.outcome = SubscribeOutcome.UNKNOWN_STOCK

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "999999") }

        assertEquals(ErrorCode.NOT_FOUND, failure.code)
        assertTrue(broadcaster.mirrored.isEmpty())
        assertTrue(broadcaster.announcements.isEmpty())
    }

    @Test
    fun `한도를 채우면 422다`() {
        store.outcome = SubscribeOutcome.LIMIT_EXCEEDED

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }

        assertEquals(ErrorCode.LIMIT_EXCEEDED, failure.code)
        assertEquals(WatchlistService.MAX_ITEMS, failure.detail?.get("limit"))
        assertTrue(broadcaster.mirrored.isEmpty())
        assertTrue(broadcaster.announcements.isEmpty())
    }

    @Test
    fun `해지하면 미러에서 빼고 알린다`() {
        broadcaster.mirrored += 1L to "005930"

        service.unsubscribe(1L, "005930")

        assertTrue(broadcaster.mirrored.isEmpty(), "미러에 종목이 남았다")
        assertEquals(
            RecordingBroadcaster.Announcement(1L, emptyList(), listOf("005930")),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `담지 않은 종목 해지도 게이트웨이에 다시 알린다`() {
        store.removes = false

        service.unsubscribe(1L, "005930")

        assertEquals(
            RecordingBroadcaster.Announcement(1L, emptyList(), listOf("005930")),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `종목코드 형식이 아니면 저장소까지 가지 않는다`() {
        val short = assertFailsWith<ApiException> { service.subscribe(1L, "5930") }
        val alpha = assertFailsWith<ApiException> { service.unsubscribe(1L, "00593A") }

        assertEquals(ErrorCode.VALIDATION_FAILED, short.code)
        assertEquals(ErrorCode.VALIDATION_FAILED, alpha.code)
        assertTrue(store.subscribeCalls.isEmpty())
    }
}
