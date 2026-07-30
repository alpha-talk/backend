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
        var unsubscribeOutcome = UnsubscribeOutcome.REMOVED
        val subscribeCalls = mutableListOf<Triple<Long, String, Int>>()

        override fun list(userId: Long): List<WatchlistItem> = emptyList()

        override fun subscribe(userId: Long, code: String, limit: Int): SubscribeOutcome {
            subscribeCalls += Triple(userId, code, limit)
            return outcome
        }

        override fun unsubscribe(userId: Long, code: String): UnsubscribeOutcome = unsubscribeOutcome

        override fun contains(userId: Long, code: String): Boolean = false
    }

    private class RecordingSynchronizer : WatchlistSynchronizer {
        val calls = mutableListOf<Pair<Long, String>>()

        override fun synchronize(userId: Long, code: String) {
            calls += userId to code
        }
    }

    private val store = FakeStore()
    private val synchronizer = RecordingSynchronizer()
    private val service = WatchlistService(store, synchronizer)

    @Test
    fun `새 종목을 담으면 미러에 넣고 게이트웨이에 알린다`() {
        val created = service.subscribe(1L, "005930")

        assertTrue(created)
        assertEquals(listOf(1L to "005930"), synchronizer.calls)
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
        assertEquals(listOf(1L to "005930"), synchronizer.calls)
    }

    @Test
    fun `없는 종목은 404다`() {
        store.outcome = SubscribeOutcome.UNKNOWN_STOCK

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "999999") }

        assertEquals(ErrorCode.NOT_FOUND, failure.code)
        assertTrue(synchronizer.calls.isEmpty())
    }

    @Test
    fun `한도를 채우면 422다`() {
        store.outcome = SubscribeOutcome.LIMIT_EXCEEDED

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }

        assertEquals(ErrorCode.LIMIT_EXCEEDED, failure.code)
        assertEquals(WatchlistService.MAX_ITEMS, failure.detail?.get("limit"))
        assertTrue(synchronizer.calls.isEmpty())
    }

    @Test
    fun `해지하면 현재 상태를 동기화한다`() {
        service.unsubscribe(1L, "005930")

        assertEquals(listOf(1L to "005930"), synchronizer.calls)
    }

    @Test
    fun `담지 않은 종목 해지도 현재 상태를 동기화한다`() {
        store.unsubscribeOutcome = UnsubscribeOutcome.ALREADY_REMOVED

        service.unsubscribe(1L, "005930")

        assertEquals(listOf(1L to "005930"), synchronizer.calls)
    }

    @Test
    fun `사용자가 사라진 구독과 해지는 401이다`() {
        store.outcome = SubscribeOutcome.OWNER_MISSING
        val subscribeFailure = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }

        store.unsubscribeOutcome = UnsubscribeOutcome.OWNER_MISSING
        val unsubscribeFailure = assertFailsWith<ApiException> { service.unsubscribe(1L, "005930") }

        assertEquals(ErrorCode.UNAUTHORIZED, subscribeFailure.code)
        assertEquals(ErrorCode.UNAUTHORIZED, unsubscribeFailure.code)
        assertTrue(synchronizer.calls.isEmpty())
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
