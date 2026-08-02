package com.alphatalk.coreapi.stockinfo

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CandleAggregatorTest {
    private fun candle(date: String, open: Long, close: Long, high: Long = maxOf(open, close), low: Long = minOf(open, close)) =
        DailyCandle(date = date, open = open, high = high, low = low, close = close, volume = 10, value = 1000)

    @Test
    fun `일봉은 count건을 오름차순으로 돌려주고 남은 과거를 표시한다`() {
        val daily = listOf(
            candle("20260710", 105, 106),
            candle("20260709", 103, 104),
            candle("20260708", 101, 102),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.DAILY, count = 2, fetchLimit = 3)

        assertEquals(listOf("20260709", "20260710"), window.candles.map(DailyCandle::date))
        assertTrue(window.hasMoreBefore)
    }

    @Test
    fun `일봉 데이터가 요청보다 적으면 있는 만큼 주고 과거 없음으로 표시한다`() {
        val daily = listOf(candle("20260710", 105, 106))

        val window = CandleAggregator.aggregate(daily, CandlePeriod.DAILY, count = 5, fetchLimit = 6)

        assertEquals(1, window.candles.size)
        assertFalse(window.hasMoreBefore)
    }

    @Test
    fun `주봉은 ISO 주 단위로 시가 종가 극값 거래량을 합성한다`() {
        val daily = listOf(
            candle("20260710", open = 110, close = 111, high = 120, low = 108),
            candle("20260709", open = 106, close = 109, high = 112, low = 105),
            candle("20260706", open = 100, close = 105, high = 107, low = 99),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.WEEKLY, count = 5, fetchLimit = 100)

        val week = window.candles.single()
        assertEquals("20260710", week.date)
        assertEquals(100, week.open)
        assertEquals(111, week.close)
        assertEquals(120, week.high)
        assertEquals(99, week.low)
        assertEquals(30, week.volume)
        assertEquals(3000, week.value)
    }

    @Test
    fun `연말과 연초가 같은 ISO 주라면 한 버킷으로 합친다`() {
        val daily = listOf(
            candle("20260102", 101, 102),
            candle("20251230", 99, 100),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.WEEKLY, count = 5, fetchLimit = 100)

        assertEquals(1, window.candles.size)
        assertEquals("20260102", window.candles.single().date)
        assertEquals(99, window.candles.single().open)
        assertEquals(102, window.candles.single().close)
    }

    @Test
    fun `월봉은 역월 단위로 묶고 최신 count개 버킷만 남긴다`() {
        val daily = listOf(
            candle("20260702", 121, 122),
            candle("20260701", 119, 120),
            candle("20260630", 117, 118),
            candle("20260501", 109, 110),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.MONTHLY, count = 2, fetchLimit = 100)

        assertEquals(listOf("20260630", "20260702"), window.candles.map(DailyCandle::date))
        assertTrue(window.hasMoreBefore)
        assertEquals(119, window.candles.last().open)
        assertEquals(122, window.candles.last().close)
    }

    @Test
    fun `조회 상한까지 다 찼으면 잘렸을 수 있는 가장 오래된 버킷을 버린다`() {
        val daily = listOf(
            candle("20260709", 106, 107),
            candle("20260708", 104, 105),
            candle("20260702", 102, 103),
            candle("20260701", 100, 101),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.WEEKLY, count = 5, fetchLimit = 4)

        assertEquals(listOf("20260709"), window.candles.map(DailyCandle::date))
        assertTrue(window.hasMoreBefore)
    }

    @Test
    fun `데이터가 다 들어왔으면 부분 버킷도 그대로 남기고 과거 없음으로 표시한다`() {
        val daily = listOf(
            candle("20260709", 106, 107),
            candle("20260702", 102, 103),
        )

        val window = CandleAggregator.aggregate(daily, CandlePeriod.WEEKLY, count = 5, fetchLimit = 100)

        assertEquals(2, window.candles.size)
        assertFalse(window.hasMoreBefore)
    }

    @Test
    fun `빈 입력은 빈 결과다`() {
        val window = CandleAggregator.aggregate(emptyList(), CandlePeriod.MONTHLY, count = 3, fetchLimit = 94)

        assertTrue(window.candles.isEmpty())
        assertFalse(window.hasMoreBefore)
    }
}
