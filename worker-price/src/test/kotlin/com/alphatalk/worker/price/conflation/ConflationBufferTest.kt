package com.alphatalk.worker.price.conflation

import com.alphatalk.kis.ws.KisTick
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ConflationBufferTest {
    private fun tick(
        code: String = "005930",
        price: Long = 71200,
        change: Long = 700,
        changeRate: Double = 0.99,
        volume: Long = 1234567,
    ) = KisTick(
        code = code,
        time = "093012",
        price = price,
        change = change,
        changeRate = changeRate,
        open = 70600,
        high = 71500,
        low = 70400,
        volume = volume,
    )

    @Test
    fun `같은 종목의 여러 틱은 최신값 하나로 합쳐진다`() {
        val buffer = ConflationBuffer()

        buffer.offer(tick(price = 71000, volume = 100))
        buffer.offer(tick(price = 71100, volume = 200))
        buffer.offer(tick(price = 71200, volume = 300))

        val drained = buffer.drainDirty()
        assertEquals(1, drained.size)
        assertEquals(71200, drained.getValue("005930").price)
        assertEquals(300, drained.getValue("005930").volume)
    }

    @Test
    fun `prevClose는 현재가에서 대비를 뺀 값이다`() {
        val buffer = ConflationBuffer()

        buffer.offer(tick(price = 71200, change = 700))
        buffer.offer(tick(code = "000660", price = 198500, change = -300))

        val drained = buffer.drainDirty()
        assertEquals(70500, drained.getValue("005930").prevClose)
        assertEquals(198800, drained.getValue("000660").prevClose)
    }

    @Test
    fun `드레인 후에는 새 틱이 올 때까지 비어 있다`() {
        val buffer = ConflationBuffer()

        buffer.offer(tick())
        assertEquals(1, buffer.drainDirty().size)
        assertTrue(buffer.drainDirty().isEmpty())

        buffer.offer(tick(price = 71300))
        assertEquals(71300, buffer.drainDirty().getValue("005930").price)
    }

    @Test
    fun `여러 종목은 각각 드레인된다`() {
        val buffer = ConflationBuffer()

        buffer.offer(tick(code = "005930"))
        buffer.offer(tick(code = "000660", price = 198500))

        val drained = buffer.drainDirty()
        assertEquals(setOf("005930", "000660"), drained.keys)
    }
}
