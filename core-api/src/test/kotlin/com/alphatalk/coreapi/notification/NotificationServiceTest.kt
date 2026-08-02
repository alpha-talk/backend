package com.alphatalk.coreapi.notification

import com.alphatalk.coreapi.search.StockCatalog
import com.alphatalk.coreapi.search.StockRef
import com.alphatalk.coreapi.stream.StreamEventType
import com.alphatalk.coreapi.stream.StreamInbox
import com.alphatalk.coreapi.stream.StreamItem
import com.alphatalk.coreapi.stream.UnreadWindow
import com.alphatalk.coreapi.subscription.WatchlistItem
import com.alphatalk.coreapi.subscription.WatchlistState
import com.alphatalk.coreapi.subscription.WatchlistStore
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class NotificationServiceTest {
    private val mapper = ObjectMapper()

    private class FakeWatchlist(private val codes: List<String>) : WatchlistStore {
        override fun list(userId: Long): List<WatchlistItem> = emptyList()
        override fun contains(userId: Long, code: String) = code in codes
        override fun count(userId: Long) = codes.size.toLong()
        override fun add(userId: Long, code: String) = throw UnsupportedOperationException()
        override fun remove(userId: Long, code: String) = throw UnsupportedOperationException()
        override fun codes(userId: Long) = codes
        override fun nextRev(userId: Long): Long = throw UnsupportedOperationException()
    }

    private class FakeInbox(
        private val counts: Map<String, Int> = emptyMap(),
        private val items: List<StreamItem> = emptyList(),
        private val latest: Map<String, String> = emptyMap(),
    ) : StreamInbox {
        var lastWindows: List<UnreadWindow>? = null
        var lastBefore: String? = null

        override fun countUnread(windows: List<UnreadWindow>, perCodeFetchLimit: Int): Map<String, Int> {
            lastWindows = windows
            return windows.associate { it.code to (counts[it.code] ?: 0) }
        }

        override fun findUnread(
            windows: List<UnreadWindow>,
            types: List<StreamEventType>,
            beforeEventId: String?,
            limit: Int,
        ): List<StreamItem> {
            lastWindows = windows
            lastBefore = beforeEventId
            return items.take(limit)
        }

        override fun hasUnreadOlderThan(windows: List<UnreadWindow>, types: List<StreamEventType>, eventId: String) = false

        override fun hasUnreadNewerThan(windows: List<UnreadWindow>, types: List<StreamEventType>, eventId: String) = false

        override fun latestEventIds(codes: Collection<String>) = latest
    }

    private class FakeCursorStore(initial: Map<Pair<Long, String>, String> = emptyMap()) : ReadCursorStore {
        val rows = initial.toMutableMap()

        override fun find(userId: Long, codes: Collection<String>): Map<String, String> =
            codes.mapNotNull { code -> rows[userId to code]?.let { code to it } }.toMap()

        override fun advance(userId: Long, code: String, eventId: String): Boolean {
            val current = rows[userId to code]
            if (current != null && current >= eventId) return false
            rows[userId to code] = eventId
            return true
        }

        override fun advanceAll(userId: Long, cursors: Map<String, String>) {
            cursors.forEach { (code, eventId) -> advance(userId, code, eventId) }
        }
    }

    private class FakeCursorCache(
        private val available: Boolean = true,
        initial: Map<Pair<Long, String>, String> = emptyMap(),
    ) : CursorCache {
        val rows = initial.toMutableMap()

        override fun read(userId: Long, codes: List<String>): Map<String, String>? {
            if (!available) return null
            return codes.mapNotNull { code -> rows[userId to code]?.let { code to it } }.toMap()
        }

        override fun advance(userId: Long, code: String, eventId: String) {
            val current = rows[userId to code]
            if (current == null || current < eventId) rows[userId to code] = eventId
        }

        override fun advanceAll(userId: Long, cursors: Map<String, String>) {
            cursors.forEach { (code, eventId) -> advance(userId, code, eventId) }
        }
    }

    private class FakeBadgeCache : BadgeCache {
        val rows = mutableMapOf<Long, BadgeResponse>()
        var evictions = 0

        override fun find(userId: Long) = rows[userId]

        override fun store(userId: Long, badge: BadgeResponse) {
            rows[userId] = badge
        }

        override fun evict(userId: Long) {
            rows.remove(userId)
            evictions++
        }
    }

    private class FakeStockCatalog(private val known: Set<String> = setOf("005930", "000660")) : StockCatalog {
        override fun existsActive(code: String) = code in known
        override fun refs(codes: Collection<String>): Map<String, StockRef> = emptyMap()
    }

    private fun item(eventId: String, code: String = "005930") = StreamItem(
        eventId = eventId,
        code = code,
        type = "NEWS",
        occurredAt = 1719500000000,
        source = "hankyung",
        payload = mapper.readTree("""{"title":"제목"}"""),
    )

    private fun service(
        watchlist: WatchlistStore = FakeWatchlist(listOf("005930", "000660")),
        inbox: StreamInbox = FakeInbox(),
        cursorStore: ReadCursorStore = FakeCursorStore(),
        cursorCache: CursorCache = FakeCursorCache(),
        badgeCache: BadgeCache = FakeBadgeCache(),
        stocks: StockCatalog = FakeStockCatalog(),
    ) = NotificationService(watchlist, inbox, cursorStore, cursorCache, badgeCache, stocks)

    @Test
    fun `배지는 종목별 미읽음을 99로 캡해 합산한다`() {
        val badge = service(
            inbox = FakeInbox(counts = mapOf("005930" to 100, "000660" to 15)),
        ).badge(1)

        assertEquals(mapOf("005930" to 99, "000660" to 15), badge.byCode)
        assertEquals(114, badge.total)
    }

    @Test
    fun `배지는 0인 종목을 생략하고 계산 결과를 캐시한다`() {
        val badgeCache = FakeBadgeCache()

        val badge = service(
            inbox = FakeInbox(counts = mapOf("005930" to 3)),
            badgeCache = badgeCache,
        ).badge(1)

        assertEquals(mapOf("005930" to 3), badge.byCode)
        assertEquals(badge, badgeCache.rows[1])
    }

    @Test
    fun `배지 캐시가 있으면 다시 집계하지 않는다`() {
        val cached = BadgeResponse(7, mapOf("005930" to 7))
        val badgeCache = FakeBadgeCache().apply { rows[1] = cached }
        val inbox = FakeInbox(counts = mapOf("005930" to 999))

        val badge = service(inbox = inbox, badgeCache = badgeCache).badge(1)

        assertEquals(cached, badge)
        assertNull(inbox.lastWindows)
    }

    @Test
    fun `커서는 캐시를 먼저 보고 없는 종목만 DB 미러에서 복구한다`() {
        val inbox = FakeInbox()
        val cursorCache = FakeCursorCache(initial = mapOf((1L to "005930") to "01J9Z800000000000000000003"))
        val cursorStore = FakeCursorStore(initial = mapOf((1L to "000660") to "01J9Z800000000000000000001"))

        service(inbox = inbox, cursorStore = cursorStore, cursorCache = cursorCache).badge(1)

        assertEquals(
            listOf(
                UnreadWindow("005930", "01J9Z800000000000000000003"),
                UnreadWindow("000660", "01J9Z800000000000000000001"),
            ),
            inbox.lastWindows,
        )
        assertEquals("01J9Z800000000000000000001", cursorCache.rows[1L to "000660"])
    }

    @Test
    fun `커서 캐시가 죽으면 DB 미러만으로 집계한다`() {
        val inbox = FakeInbox()
        val cursorStore = FakeCursorStore(initial = mapOf((1L to "005930") to "01J9Z800000000000000000002"))

        service(inbox = inbox, cursorStore = cursorStore, cursorCache = FakeCursorCache(available = false)).badge(1)

        assertEquals(
            listOf(UnreadWindow("005930", "01J9Z800000000000000000002"), UnreadWindow("000660", null)),
            inbox.lastWindows,
        )
    }

    @Test
    fun `알림 목록은 최신부터 내려가고 pageInfo를 채운다`() {
        val items = listOf(item("01J9Z800000000000000000005"), item("01J9Z800000000000000000004"))

        val page = service(inbox = FakeInbox(items = items)).list(1, null, null, null)

        assertEquals("01J9Z800000000000000000005", page.pageInfo.newest)
        assertEquals("01J9Z800000000000000000004", page.pageInfo.oldest)
        assertFalse(page.pageInfo.hasMoreBefore)
        assertFalse(page.pageInfo.hasMoreAfter)
    }

    @Test
    fun `알림 목록의 잘못된 type은 400이다`() {
        val e = assertFailsWith<ApiException> { service().list(1, "news,unknown", null, null) }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `커서 전진은 저장소와 캐시에 쓰고 배지 캐시를 비운다`() {
        val cursorStore = FakeCursorStore()
        val cursorCache = FakeCursorCache()
        val badgeCache = FakeBadgeCache().apply { rows[1] = BadgeResponse(1, mapOf("005930" to 1)) }

        service(cursorStore = cursorStore, cursorCache = cursorCache, badgeCache = badgeCache)
            .advanceCursor(1, "005930", "01J9Z800000000000000000005")

        assertEquals("01J9Z800000000000000000005", cursorStore.rows[1L to "005930"])
        assertEquals("01J9Z800000000000000000005", cursorCache.rows[1L to "005930"])
        assertNull(badgeCache.rows[1])
    }

    @Test
    fun `커서 전진은 lastEventId가 없거나 형식이 틀리면 400이다`() {
        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().advanceCursor(1, "005930", null) }.code,
        )
        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().advanceCursor(1, "005930", "not-a-ulid") }.code,
        )
    }

    @Test
    fun `없는 종목의 커서 전진은 404다`() {
        val e = assertFailsWith<ApiException> {
            service().advanceCursor(1, "999999", "01J9Z800000000000000000005")
        }

        assertEquals(ErrorCode.NOT_FOUND, e.code)
    }

    @Test
    fun `모두 읽음은 종목별 최신 이벤트로 커서를 옮긴다`() {
        val latest = mapOf(
            "005930" to "01J9Z800000000000000000009",
            "000660" to "01J9Z800000000000000000007",
        )
        val cursorStore = FakeCursorStore(initial = mapOf((1L to "005930") to "01J9Z800000000000000000002"))
        val cursorCache = FakeCursorCache()
        val badgeCache = FakeBadgeCache().apply { rows[1] = BadgeResponse(9, mapOf()) }

        service(
            inbox = FakeInbox(latest = latest),
            cursorStore = cursorStore,
            cursorCache = cursorCache,
            badgeCache = badgeCache,
        ).readAll(1)

        assertEquals("01J9Z800000000000000000009", cursorStore.rows[1L to "005930"])
        assertEquals("01J9Z800000000000000000007", cursorStore.rows[1L to "000660"])
        assertEquals("01J9Z800000000000000000009", cursorCache.rows[1L to "005930"])
        assertTrue(badgeCache.rows.isEmpty())
    }
}
