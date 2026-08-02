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
        override fun list(userId: Long): List<WatchlistItem> = emptyList()

        override fun contains(userId: Long, code: String): Boolean = false

        override fun count(userId: Long): Long = 0

        override fun add(userId: Long, code: String) = Unit

        override fun remove(userId: Long, code: String): Boolean = false

        override fun codes(userId: Long): List<String> = emptyList()

        override fun nextRev(userId: Long): Long = 0
    }

    private class FakeCommand : WatchlistCommand {
        var subscribeChange = SubscribeChange(SubscribeOutcome.ADDED, WatchlistState(1, listOf("005930")))
        var unsubscribeChange = UnsubscribeChange(UnsubscribeOutcome.REMOVED, WatchlistState(2, emptyList()))
        val subscribeCalls = mutableListOf<Triple<Long, String, Int>>()
        val unsubscribeCalls = mutableListOf<Pair<Long, String>>()

        override fun subscribe(userId: Long, code: String, limit: Int): SubscribeChange {
            subscribeCalls += Triple(userId, code, limit)
            return subscribeChange
        }

        override fun unsubscribe(userId: Long, code: String): UnsubscribeChange {
            unsubscribeCalls += userId to code
            return unsubscribeChange
        }
    }

    private class RecordingMirror : WatchlistMirrorSync {
        val synced = mutableListOf<Pair<Long, MirrorChange>>()

        override fun sync(userId: Long, change: MirrorChange): Boolean {
            synced += userId to change
            return true
        }
    }

    private val command = FakeCommand()
    private val mirror = RecordingMirror()
    private val service = WatchlistService(FakeStore(), command, mirror)

    @Test
    fun `새 종목을 담으면 커밋된 상태로 미러를 동기화한다`() {
        val state = WatchlistState(7, listOf("005930", "000660"))
        command.subscribeChange = SubscribeChange(SubscribeOutcome.ADDED, state)

        val created = service.subscribe(1L, "005930")

        assertTrue(created)
        assertEquals(1L to MirrorChange(state, addedCode = "005930"), mirror.synced.single())
    }

    @Test
    fun `한도는 서비스가 정해 커맨드에 넘긴다`() {
        service.subscribe(1L, "005930")

        assertEquals(Triple(1L, "005930", WatchlistService.MAX_ITEMS), command.subscribeCalls.single())
    }

    @Test
    fun `이미 담긴 종목 재요청도 미러를 다시 동기화한다`() {
        val state = WatchlistState(8, listOf("005930"))
        command.subscribeChange = SubscribeChange(SubscribeOutcome.ALREADY_SUBSCRIBED, state)

        val created = service.subscribe(1L, "005930")

        assertFalse(created)
        assertEquals(1L to MirrorChange(state, addedCode = "005930"), mirror.synced.single())
    }

    @Test
    fun `없는 종목은 404고 미러를 건드리지 않는다`() {
        command.subscribeChange = SubscribeChange(SubscribeOutcome.UNKNOWN_STOCK)

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "999999") }

        assertEquals(ErrorCode.NOT_FOUND, failure.code)
        assertTrue(mirror.synced.isEmpty())
    }

    @Test
    fun `한도를 채우면 422고 미러를 건드리지 않는다`() {
        command.subscribeChange = SubscribeChange(SubscribeOutcome.LIMIT_EXCEEDED)

        val failure = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }

        assertEquals(ErrorCode.LIMIT_EXCEEDED, failure.code)
        assertEquals(WatchlistService.MAX_ITEMS, failure.detail?.get("limit"))
        assertTrue(mirror.synced.isEmpty())
    }

    @Test
    fun `사라진 사용자는 401이다`() {
        command.subscribeChange = SubscribeChange(SubscribeOutcome.OWNER_MISSING)
        command.unsubscribeChange = UnsubscribeChange(UnsubscribeOutcome.OWNER_MISSING)

        val put = assertFailsWith<ApiException> { service.subscribe(1L, "005930") }
        val delete = assertFailsWith<ApiException> { service.unsubscribe(1L, "005930") }

        assertEquals(ErrorCode.UNAUTHORIZED, put.code)
        assertEquals(ErrorCode.UNAUTHORIZED, delete.code)
        assertTrue(mirror.synced.isEmpty())
    }

    @Test
    fun `해지하면 커밋된 상태로 미러를 동기화한다`() {
        val state = WatchlistState(9, listOf("000660"))
        command.unsubscribeChange = UnsubscribeChange(UnsubscribeOutcome.REMOVED, state)

        service.unsubscribe(1L, "005930")

        assertEquals(1L to MirrorChange(state, removedCode = "005930"), mirror.synced.single())
    }

    @Test
    fun `담지 않은 종목 해지도 미러를 다시 동기화한다`() {
        val state = WatchlistState(10, emptyList())
        command.unsubscribeChange = UnsubscribeChange(UnsubscribeOutcome.ALREADY_REMOVED, state)

        service.unsubscribe(1L, "000660")

        assertEquals(1L to MirrorChange(state, removedCode = "000660"), mirror.synced.single())
    }

    @Test
    fun `종목코드 형식이 아니면 커맨드까지 가지 않는다`() {
        val short = assertFailsWith<ApiException> { service.subscribe(1L, "5930") }
        val alpha = assertFailsWith<ApiException> { service.unsubscribe(1L, "00593A") }

        assertEquals(ErrorCode.VALIDATION_FAILED, short.code)
        assertEquals(ErrorCode.VALIDATION_FAILED, alpha.code)
        assertTrue(command.subscribeCalls.isEmpty())
        assertTrue(command.unsubscribeCalls.isEmpty())
    }
}
