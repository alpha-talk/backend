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
        val rows = linkedSetOf<Pair<Long, String>>()

        override fun list(userId: Long) = rows.filter { it.first == userId }
            .map { WatchlistItem(it.second, "이름", "KOSPI", 0L) }

        override fun state(userId: Long, code: String) = WatchlistState(
            total = rows.count { it.first == userId },
            subscribed = (userId to code) in rows,
        )

        override fun add(userId: Long, code: String) = rows.add(userId to code)

        override fun remove(userId: Long, code: String) = rows.remove(userId to code)
    }

    private class FakeCatalog(private val known: Set<String>) : StockCatalog {
        override fun exists(code: String) = code in known
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
    private val catalog = FakeCatalog(setOf("005930", "000660"))
    private val broadcaster = RecordingBroadcaster()
    private val service = WatchlistService(store, catalog, broadcaster, broadcaster)

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
    fun `이미 담긴 종목은 다시 알리지 않는다`() {
        service.subscribe(1L, "005930")
        broadcaster.announcements.clear()

        val created = service.subscribe(1L, "005930")

        assertFalse(created)
        assertTrue(broadcaster.announcements.isEmpty(), "변경이 없는데 전역 채널에 발행했다")
    }

    @Test
    fun `재요청은 어긋난 미러를 되돌려 놓는다`() {
        service.subscribe(1L, "005930")
        broadcaster.mirrored.clear()

        service.subscribe(1L, "005930")

        assertEquals(setOf(1L to "005930"), broadcaster.mirrored, "미러가 복구되지 않았다")
    }

    @Test
    fun `없는 종목은 담지 못한다`() {
        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "999999") }

        assertEquals(ErrorCode.NOT_FOUND, failure.code)
        assertTrue(broadcaster.mirrored.isEmpty())
        assertTrue(broadcaster.announcements.isEmpty())
    }

    @Test
    fun `상장폐지 등으로 마스터에서 빠진 종목도 해지할 수 있다`() {
        store.rows += 1L to "111111"

        service.unsubscribe(1L, "111111")

        assertEquals(
            RecordingBroadcaster.Announcement(1L, emptyList(), listOf("111111")),
            broadcaster.announcements.single(),
        )
    }

    @Test
    fun `한도를 채우면 더 담지 못한다`() {
        repeat(WatchlistService.MAX_ITEMS) { store.rows += 1L to "9%05d".format(it) }

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }

        assertEquals(ErrorCode.LIMIT_EXCEEDED, failure.code)
        assertEquals(WatchlistService.MAX_ITEMS, failure.detail?.get("limit"))
        assertTrue(broadcaster.announcements.isEmpty())
    }

    @Test
    fun `한도가 찬 뒤에도 이미 담긴 종목 재요청은 통과한다`() {
        store.rows += 1L to "005930"
        repeat(WatchlistService.MAX_ITEMS - 1) { store.rows += 1L to "9%05d".format(it) }

        assertFalse(service.subscribe(1L, "005930"))
    }

    @Test
    fun `한도는 사용자별로 센다`() {
        repeat(WatchlistService.MAX_ITEMS) { store.rows += 2L to "9%05d".format(it) }

        assertTrue(service.subscribe(1L, "005930"))
    }

    @Test
    fun `담지 않은 종목 해지는 알리지 않는다`() {
        service.unsubscribe(1L, "005930")

        assertTrue(broadcaster.announcements.isEmpty(), "변경이 없는데 전역 채널에 발행했다")
    }

    @Test
    fun `해지는 미러에 남은 찌꺼기까지 걷어낸다`() {
        broadcaster.mirrored += 1L to "005930"

        service.unsubscribe(1L, "005930")

        assertTrue(broadcaster.mirrored.isEmpty(), "미러에 종목이 남았다")
    }

    @Test
    fun `종목코드 형식이 아니면 거부한다`() {
        val short = assertFailsWith<ApiException> { service.subscribe(1L, "5930") }
        val alpha = assertFailsWith<ApiException> { service.unsubscribe(1L, "00593A") }

        assertEquals(ErrorCode.VALIDATION_FAILED, short.code)
        assertEquals(ErrorCode.VALIDATION_FAILED, alpha.code)
    }
}
