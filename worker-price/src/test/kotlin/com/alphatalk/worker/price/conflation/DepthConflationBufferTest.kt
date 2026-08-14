package com.alphatalk.worker.price.conflation

import com.alphatalk.kis.ws.KisDepth
import com.alphatalk.kis.ws.KisDepthLevel
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DepthConflationBufferTest {
    private fun depth(code: String, bestAsk: Long) = KisDepth(
        code = code,
        time = "093012",
        asks = listOf(KisDepthLevel(bestAsk, 120), KisDepthLevel(bestAsk + 100, 250), KisDepthLevel(0, 0)),
        bids = listOf(KisDepthLevel(bestAsk - 100, 340), KisDepthLevel(0, 0)),
    )

    @Test
    fun `종목별 마지막 호가만 남기고 가격 0 단계는 payload에서 뺀다`() {
        val buffer = DepthConflationBuffer()
        buffer.offer(depth("005930", 71300))
        buffer.offer(depth("005930", 71400))

        val drained = buffer.drainDirty()

        val data = drained.getValue("005930")
        assertEquals(listOf(listOf(71400L, 120L), listOf(71500L, 250L)), data.asks)
        assertEquals(listOf(listOf(71300L, 340L)), data.bids)
    }

    @Test
    fun `드레인 뒤 새 호가가 없으면 재발행 대상이 없다`() {
        val buffer = DepthConflationBuffer()
        buffer.offer(depth("005930", 71300))
        buffer.drainDirty()

        assertTrue(buffer.drainDirty().isEmpty())
    }

    @Test
    fun `여러 종목은 각자 최신 호가로 드레인된다`() {
        val buffer = DepthConflationBuffer()
        buffer.offer(depth("005930", 71300))
        buffer.offer(depth("000660", 198600))

        val drained = buffer.drainDirty()

        assertEquals(setOf("005930", "000660"), drained.keys)
        assertEquals(listOf(198600L, 120L), drained.getValue("000660").asks[0])
    }
}
