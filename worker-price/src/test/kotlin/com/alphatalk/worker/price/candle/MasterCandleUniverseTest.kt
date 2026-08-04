package com.alphatalk.worker.price.candle

import kotlin.test.Test
import kotlin.test.assertEquals

class MasterCandleUniverseTest {
    @Test
    fun `stock_master가 있으면 활성 전 종목을 쓴다`() {
        val universe = MasterCandleUniverse(
            activeCodes = { listOf("005930", "000660") },
            fallback = { setOf("035420") },
        )

        assertEquals(setOf("005930", "000660"), universe.symbols())
    }

    @Test
    fun `stock_master가 비어 있으면 수요 종목으로 대체한다`() {
        val universe = MasterCandleUniverse(
            activeCodes = { emptyList() },
            fallback = { setOf("035420") },
        )

        assertEquals(setOf("035420"), universe.symbols())
    }

    @Test
    fun `stock_master 조회가 실패해도 수요 종목으로 대체한다`() {
        val universe = MasterCandleUniverse(
            activeCodes = { throw IllegalStateException("db down") },
            fallback = { setOf("035420") },
        )

        assertEquals(setOf("035420"), universe.symbols())
    }
}
