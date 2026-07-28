package com.alphatalk.coreapi.search

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class StockSearchServiceTest {
    private class RecordingStore : StockSearchStore {
        val calls = mutableListOf<Pair<String, Int>>()

        override fun search(query: String, limit: Int): List<StockSummary> {
            calls += query to limit
            return emptyList()
        }
    }

    private val store = RecordingStore()
    private val service = StockSearchService(store)

    @Test
    fun `limit을 주지 않으면 10건을 찾는다`() {
        service.search("삼성", null)

        assertEquals("삼성" to 10, store.calls.single())
    }

    @Test
    fun `검색어 앞뒤 공백은 버린다`() {
        service.search("  삼성  ", null)

        assertEquals("삼성", store.calls.single().first)
    }

    @Test
    fun `검색어가 비어 있으면 거부한다`() {
        val blank = assertFailsWith<ApiException> { service.search("   ", null) }
        val missing = assertFailsWith<ApiException> { service.search(null, null) }

        assertEquals(ErrorCode.VALIDATION_FAILED, blank.code)
        assertEquals(ErrorCode.VALIDATION_FAILED, missing.code)
        assertEquals("q", blank.detail?.get("field"))
    }

    @Test
    fun `limit이 허용 범위를 벗어나면 거부한다`() {
        assertFailsWith<ApiException> { service.search("삼성", 0) }
        assertFailsWith<ApiException> { service.search("삼성", 31) }

        assertEquals(0, store.calls.size)
    }

    @Test
    fun `허용 범위 경계는 통과한다`() {
        service.search("삼성", 1)
        service.search("삼성", 30)

        assertEquals(listOf(1, 30), store.calls.map { it.second })
    }
}
