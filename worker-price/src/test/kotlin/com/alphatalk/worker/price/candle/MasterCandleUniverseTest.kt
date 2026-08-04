package com.alphatalk.worker.price.candle

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class MasterCandleUniverseTest {
    private fun universe(
        activeCodes: () -> List<String>,
        fallback: () -> Set<String> = { setOf("035420") },
    ) = MasterCandleUniverse(
        activeCodes = activeCodes,
        fallback = fallback,
        retryDelayMillis = 0,
        sleep = {},
    )

    @Test
    fun `stock_master가 있으면 활성 전 종목을 쓴다`() {
        assertEquals(setOf("005930", "000660"), universe({ listOf("005930", "000660") }).symbols())
    }

    @Test
    fun `stock_master가 비어 있으면 수요 종목으로 대체한다 - 초기 구축 전 정상 경로`() {
        assertEquals(setOf("035420"), universe({ emptyList() }).symbols())
    }

    @Test
    fun `조회가 일시 실패하면 재시도해 복구한다`() {
        var attempts = 0

        val symbols = universe({
            attempts += 1
            if (attempts < 3) throw IllegalStateException("db down") else listOf("005930")
        }).symbols()

        assertEquals(setOf("005930"), symbols)
        assertEquals(3, attempts)
    }

    @Test
    fun `조회가 계속 실패하면 수요로 축소하지 않고 예외로 중단한다`() {
        var attempts = 0

        val e = assertFailsWith<CandleUniverseUnavailableException> {
            universe({
                attempts += 1
                throw IllegalStateException("db down")
            }, fallback = { error("장애 시 수요 대체는 금지된다") }).symbols()
        }

        assertEquals(3, attempts)
        assertTrue("db down" in e.cause?.message.orEmpty())
    }
}
