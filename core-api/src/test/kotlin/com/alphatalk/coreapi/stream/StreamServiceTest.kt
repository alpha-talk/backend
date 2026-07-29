package com.alphatalk.coreapi.stream

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import com.fasterxml.jackson.databind.ObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StreamServiceTest {
    private val mapper = ObjectMapper()

    private class RecordingStreamStore(private val items: List<StreamItem> = emptyList()) : StreamStore {
        var lastQuery: StreamQuery? = null
        var olderAsked = false
        var newerAsked = false

        override fun find(query: StreamQuery): List<StreamItem> {
            lastQuery = query
            return items
        }

        override fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>): Boolean {
            olderAsked = true
            return true
        }

        override fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>): Boolean {
            newerAsked = true
            return false
        }
    }

    private class StubQuoteStore(
        private val live: QuoteResponse? = null,
        private val candle: QuoteResponse? = null,
    ) : QuoteStore {
        override fun liveQuote(code: String) = live
        override fun lastCandleQuote(code: String) = candle
    }

    private fun item(eventId: String, type: String = "NEWS") = StreamItem(
        eventId = eventId,
        code = "005930",
        type = type,
        occurredAt = 1719500000000,
        source = "hankyung",
        payload = mapper.readTree("""{"title":"제목"}"""),
    )

    private fun service(
        store: StreamStore = RecordingStreamStore(),
        quotes: QuoteStore = StubQuoteStore(),
    ) = StreamService(store, quotes)

    @Test
    fun `기본값은 최신부터 50건이다`() {
        val store = RecordingStreamStore()

        service(store).read("005930", null, null, null, null)

        val query = store.lastQuery!!
        assertEquals(CursorDirection.BEFORE, query.direction)
        assertEquals(50, query.limit)
        assertNull(query.cursor)
        assertTrue(query.types.isEmpty())
    }

    @Test
    fun `types는 소문자 토큰을 DB 타입으로 옮긴다`() {
        val store = RecordingStreamStore()

        service(store).read("005930", null, null, null, "news,ai,post")

        assertEquals(
            listOf("NEWS", "AI", "POST"),
            store.lastQuery!!.types.map(StreamEventType::storedType),
        )
    }

    @Test
    fun `같은 type을 여러 번 줘도 한 번만 센다`() {
        val store = RecordingStreamStore()

        service(store).read("005930", null, null, null, "news, news ,NEWS")

        assertEquals(1, store.lastQuery!!.types.size)
    }

    @Test
    fun `모르는 type은 거부한다`() {
        val e = assertFailsWith<ApiException> { service().read("005930", null, null, null, "news,unknown") }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
        assertEquals("types", e.detail?.get("field"))
    }

    @Test
    fun `종목 코드는 6자리 숫자여야 한다`() {
        assertFailsWith<ApiException> { service().read("00593", null, null, null, null) }
        assertFailsWith<ApiException> { service().read("abcdef", null, null, null, null) }
    }

    @Test
    fun `cursor는 ULID 형식만 받는다`() {
        val e = assertFailsWith<ApiException> {
            service().read("005930", "not-a-ulid", null, null, null)
        }

        assertEquals("cursor", e.detail?.get("field"))
    }

    @Test
    fun `빈 cursor는 없는 것으로 본다`() {
        val store = RecordingStreamStore()

        service(store).read("005930", "  ", null, null, null)

        assertNull(store.lastQuery!!.cursor)
    }

    @Test
    fun `direction은 before after만 받는다`() {
        val store = RecordingStreamStore()

        service(store).read("005930", null, "after", null, null)
        assertEquals(CursorDirection.AFTER, store.lastQuery!!.direction)

        assertFailsWith<ApiException> { service().read("005930", null, "sideways", null, null) }
    }

    @Test
    fun `limit 범위를 벗어나면 거부한다`() {
        assertFailsWith<ApiException> { service().read("005930", null, null, 0, null) }
        assertFailsWith<ApiException> { service().read("005930", null, null, 101, null) }
    }

    @Test
    fun `결과가 비면 pageInfo도 비고 추가 조회를 하지 않는다`() {
        val store = RecordingStreamStore(emptyList())

        val page = service(store).read("005930", null, null, null, null)

        assertNull(page.pageInfo.oldest)
        assertNull(page.pageInfo.newest)
        assertEquals(false, page.pageInfo.hasMoreBefore)
        assertEquals(false, page.pageInfo.hasMoreAfter)
        assertTrue(!store.olderAsked && !store.newerAsked)
    }

    @Test
    fun `pageInfo는 정렬 방향과 무관하게 최소 최대를 고른다`() {
        val store = RecordingStreamStore(listOf(item("01J9Z8000000000000000000B"), item("01J9Z8000000000000000000A")))

        val page = service(store).read("005930", null, null, null, null)

        assertEquals("01J9Z8000000000000000000A", page.pageInfo.oldest)
        assertEquals("01J9Z8000000000000000000B", page.pageInfo.newest)
        assertEquals(true, page.pageInfo.hasMoreBefore)
        assertEquals(false, page.pageInfo.hasMoreAfter)
    }

    @Test
    fun `시세는 실시간을 먼저 보고 없으면 일봉으로 답한다`() {
        val live = QuoteResponse("005930", 71200, 70500, 700, 0.99, 70600, 71500, 70400, 100, 1L, false)
        val candle = QuoteResponse("005930", 70000, 69000, 1000, 1.45, 69500, 70100, 69000, 200, 2L, true)

        assertEquals(false, service(quotes = StubQuoteStore(live, candle)).quote("005930").delayed)
        assertEquals(true, service(quotes = StubQuoteStore(null, candle)).quote("005930").delayed)
    }

    @Test
    fun `실시간도 일봉도 없으면 404다`() {
        val e = assertFailsWith<ApiException> { service(quotes = StubQuoteStore(null, null)).quote("005930") }

        assertEquals(ErrorCode.NOT_FOUND, e.code)
    }
}
