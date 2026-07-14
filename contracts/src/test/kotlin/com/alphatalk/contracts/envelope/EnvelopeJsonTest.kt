package com.alphatalk.contracts.envelope

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class EnvelopeJsonTest {
    private val mapper = jacksonObjectMapper()

    @Test
    fun `quote 봉투 왕복`() {
        val envelope = Envelope(
            type = "quote",
            code = "005930",
            ts = 1719600000000,
            data = QuoteData(
                price = 71200, prevClose = 70500, change = 700, changeRate = 0.99,
                volume = 1234567, open = 70600, high = 71500, low = 70400,
            ),
        )
        val json = mapper.writeValueAsString(envelope)
        val back = mapper.readValue<Envelope<QuoteData>>(json)
        assertEquals(envelope, back)

        assertFalse(json.contains("eventId"))
    }

    @Test
    fun `stream 봉투 왕복 - eventId 필수 케이스`() {
        val envelope = Envelope(
            type = "stream",
            code = "000660",
            eventId = "01J9Z8X7ABCDEFGHJKMNPQRSTV",
            ts = 1719600000000,
            data = StreamData(
                category = "news", title = "제목", summary = "요약",
                sourceUrl = "https://example.com", occurredAt = 1719500000000,
            ),
        )
        val back = mapper.readValue<Envelope<StreamData>>(mapper.writeValueAsString(envelope))
        assertEquals(envelope, back)
    }

    @Test
    fun `watchlist updated 왕복 - 기본값 처리`() {
        val json = """{"userId":123,"added":["005930"],"ts":1719600000000}"""
        val parsed = mapper.readValue<WatchlistUpdated>(json)
        assertEquals(WatchlistUpdated(userId = 123, added = listOf("005930"), ts = 1719600000000), parsed)
        assertEquals(emptyList(), parsed.removed)
    }
}
