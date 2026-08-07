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
    fun `stream 봉투 v0_5 확장 필드 왕복`() {
        val envelope = Envelope(
            type = "stream",
            code = "005930",
            eventId = "01J9Z8X7ABCDEFGHJKMNPQRSTV",
            ts = 1719600000000,
            data = StreamData(
                category = "news",
                title = "한은, 기준금리 25bp 인상",
                summary = "요약",
                sourceUrl = "https://example.com",
                occurredAt = 1719500000000,
                sentiment = Sentiment.POSITIVE.name,
                scope = NewsScope.SECTOR.name,
                sector = SectorRef(code = "27", name = "은행"),
                sources = listOf(SourceRef(name = "한국경제", url = "https://example.com/a")),
            ),
        )
        val back = mapper.readValue<Envelope<StreamData>>(mapper.writeValueAsString(envelope))
        assertEquals(envelope, back)
    }

    @Test
    fun `stream v0_5 확장 필드 미사용 시 직렬화 생략 - 비파괴`() {
        val json = mapper.writeValueAsString(
            StreamData(category = "news", title = "제목", occurredAt = 1719500000000),
        )
        assertFalse(json.contains("sentiment"))
        assertFalse(json.contains("scope"))
        assertFalse(json.contains("sector"))
        assertFalse(json.contains("sources"))
        assertFalse(json.contains("digest"))
        assertFalse(json.contains("kind"))
        assertFalse(json.contains("opinion"))
    }

    @Test
    fun `stream 봉투 투자의견 왕복 - v0_7`() {
        val envelope = Envelope(
            type = "stream",
            code = "005930",
            eventId = "01J9Z8X7ABCDEFGHJKMNPQRSTV",
            ts = 1719600000000,
            data = StreamData(
                category = "report",
                title = "미래에셋 투자의견 매수",
                occurredAt = 1719500000000,
                kind = "opinion",
                opinion = OpinionData(
                    brokerCode = "00005",
                    brokerName = "미래에셋",
                    rating = "매수",
                    previousRating = "중립",
                    targetPrice = 95000,
                    businessDate = "20260727",
                ),
            ),
        )
        val back = mapper.readValue<Envelope<StreamData>>(mapper.writeValueAsString(envelope))
        assertEquals(envelope, back)
    }

    @Test
    fun `투자의견 optional 필드 null 생략 - 필수 필드만 직렬화`() {
        val json = mapper.writeValueAsString(
            OpinionData(brokerCode = "00088", rating = "NotRated", businessDate = "20260807"),
        )
        assertEquals("""{"brokerCode":"00088","rating":"NotRated","businessDate":"20260807"}""", json)
    }

    @Test
    fun `ai 봉투 digest 왕복`() {
        val digest = DigestData(
            date = "2026-07-16",
            positives = listOf(DigestData.Item(title = "3나노 수주", line = "한 줄", eventId = "01J9Z8X7ABCDEFGHJKMNPQRSTV")),
            negatives = emptyList(),
            sectorIssues = listOf(
                DigestData.SectorIssue(title = "기준금리 인상", line = "이자이익 개선", sentiment = Sentiment.POSITIVE.name),
            ),
            marketIssues = listOf(DigestData.MarketIssue(title = "외국인 순매도", line = "지속")),
            neutralCount = 4,
            newsCount = 12,
        )
        val data = StreamData(category = "ai", title = "삼성전자 데일리 브리핑", occurredAt = 1719500000000, digest = digest)
        val back = mapper.readValue<StreamData>(mapper.writeValueAsString(data))
        assertEquals(data, back)
    }

    @Test
    fun `watchlist updated 왕복 - 기본값 처리`() {
        val json = """{"userId":123,"added":["005930"],"ts":1719600000000}"""
        val parsed = mapper.readValue<WatchlistUpdated>(json)
        assertEquals(WatchlistUpdated(userId = 123, added = listOf("005930"), ts = 1719600000000), parsed)
        assertEquals(emptyList(), parsed.removed)
    }
}
