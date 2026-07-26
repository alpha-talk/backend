package com.alphatalk.kis.ws

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

class KisFrameParserTest {
    private fun fixture(name: String): String =
        javaClass.getResource("/fixtures/$name")!!.readText()

    @Test
    fun `단건 체결 프레임을 틱으로 파싱한다`() {
        val frame = KisFrameParser.parse(fixture("h0stcnt0-single.txt"))

        val ticks = assertIs<KisFrame.Ticks>(frame).ticks
        assertEquals(1, ticks.size)
        val tick = ticks[0]
        assertEquals("005930", tick.code)
        assertEquals("093012", tick.time)
        assertEquals(71200, tick.price)
        assertEquals(700, tick.change)
        assertEquals(0.99, tick.changeRate)
        assertEquals(70600, tick.open)
        assertEquals(71500, tick.high)
        assertEquals(70400, tick.low)
        assertEquals(1234567, tick.volume)
    }

    @Test
    fun `2건 이어붙은 프레임은 필드 수로 분할하고 하락 부호를 음수로 만든다`() {
        val frame = KisFrameParser.parse(fixture("h0stcnt0-double.txt"))

        val ticks = assertIs<KisFrame.Ticks>(frame).ticks
        assertEquals(2, ticks.size)
        assertEquals("005930", ticks[0].code)
        assertEquals(700, ticks[0].change)
        assertEquals("000660", ticks[1].code)
        assertEquals(198500, ticks[1].price)
        assertEquals(-300, ticks[1].change)
        assertEquals(-0.15, ticks[1].changeRate)
        assertEquals(987654, ticks[1].volume)
    }

    @Test
    fun `PINGPONG 프레임을 식별한다`() {
        val frame = KisFrameParser.parse(fixture("pingpong.txt"))

        assertIs<KisFrame.PingPong>(frame)
    }

    @Test
    fun `구독 성공 응답을 Control로 파싱한다`() {
        val frame = KisFrameParser.parse(fixture("subscribe-success.txt"))

        val control = assertIs<KisFrame.Control>(frame)
        assertEquals("H0STCNT0", control.trId)
        assertEquals("005930", control.trKey)
        assertTrue(control.success)
    }

    @Test
    fun `구독 실패 응답은 success가 false다`() {
        val frame = KisFrameParser.parse(fixture("subscribe-failure.txt"))

        val control = assertIs<KisFrame.Control>(frame)
        assertFalse(control.success)
    }

    @Test
    fun `암호화 프레임은 드랍 신호로 바꾼다`() {
        val frame = KisFrameParser.parse("1|H0STCNT0|001|garbled-cipher-text")

        assertEquals(KisFrame.EncryptedDropped("H0STCNT0"), frame)
    }

    @Test
    fun `형식이 어긋난 프레임은 Unknown이다`() {
        assertIs<KisFrame.Unknown>(KisFrameParser.parse("0|H0STCNT0|001|too^few^fields"))
        assertIs<KisFrame.Unknown>(KisFrameParser.parse("0|H0STASP0|001|005930^093012"))
        assertIs<KisFrame.Unknown>(KisFrameParser.parse("not-a-frame"))
        assertIs<KisFrame.Unknown>(KisFrameParser.parse("{invalid json"))
    }
}
