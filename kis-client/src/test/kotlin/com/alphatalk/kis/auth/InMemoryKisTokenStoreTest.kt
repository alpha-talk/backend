package com.alphatalk.kis.auth

import java.time.Duration
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class InMemoryKisTokenStoreTest {
    @Test
    fun `만료된 토큰은 조회되지 않는다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val store = InMemoryKisTokenStore { now }

        store.put("k", "T1", Duration.ofSeconds(60))
        assertEquals("T1", store.get("k"))

        now += Duration.ofSeconds(61)
        assertNull(store.get("k"))
    }

    @Test
    fun `락은 TTL 내 중복 획득이 안 되고 만료 후 다시 획득된다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val store = InMemoryKisTokenStore { now }

        assertNotNull(store.tryLock("k", Duration.ofSeconds(5)))
        assertNull(store.tryLock("k", Duration.ofSeconds(5)))

        now += Duration.ofSeconds(6)
        assertNotNull(store.tryLock("k", Duration.ofSeconds(5)))
    }

    @Test
    fun `unlock 후 즉시 재획득된다`() {
        val store = InMemoryKisTokenStore()

        val lockToken = store.tryLock("k", Duration.ofSeconds(5))
        assertNotNull(lockToken)
        store.unlock("k", lockToken)
        assertNotNull(store.tryLock("k", Duration.ofSeconds(5)))
    }

    @Test
    fun `만료 후 넘어간 락은 이전 소유자가 해제하지 못한다`() {
        var now = Instant.parse("2026-07-27T00:00:00Z")
        val store = InMemoryKisTokenStore { now }

        val first = store.tryLock("k", Duration.ofSeconds(5))
        assertNotNull(first)
        now += Duration.ofSeconds(6)
        val second = store.tryLock("k", Duration.ofSeconds(60))
        assertNotNull(second)

        store.unlock("k", first)

        assertNull(store.tryLock("k", Duration.ofSeconds(5)))
        store.unlock("k", second)
        assertNotNull(store.tryLock("k", Duration.ofSeconds(5)))
    }
}
