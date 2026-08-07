package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.envelope.OpinionData
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNull

class OpinionObservationTest {
    private fun observation(
        brokerName: String? = "미래에셋",
        rating: String = "매수",
        previousRating: String? = "중립",
        targetPrice: Long? = 95000,
    ) = OpinionObservation(
        code = "005930",
        businessDate = "20260807",
        brokerCode = "00005",
        brokerName = brokerName,
        rating = rating,
        previousRating = previousRating,
        targetPrice = targetPrice,
        contentHash = OpinionObservation.contentHash(rating, previousRating, targetPrice),
        collectedAt = Instant.ofEpochMilli(1785106800000),
    )

    @Test
    fun `content_hash는 텍스트 트림·널 정규화 후 동일하다`() {
        assertEquals(
            OpinionObservation.contentHash("매수", "중립", 95000),
            OpinionObservation.contentHash(" 매수 ", " 중립 ", 95000),
        )
        assertEquals(
            OpinionObservation.contentHash("매수", null, null),
            OpinionObservation.contentHash("매수", null, null),
        )
    }

    @Test
    fun `의견·직전의견·목표가가 다르면 해시가 갈린다`() {
        val base = OpinionObservation.contentHash("매수", "중립", 95000)
        assertNotEquals(base, OpinionObservation.contentHash("중립", "중립", 95000))
        assertNotEquals(base, OpinionObservation.contentHash("매수", "매수", 95000))
        assertNotEquals(base, OpinionObservation.contentHash("매수", "중립", 90000))
        assertNotEquals(base, OpinionObservation.contentHash("매수", "중립", null))
    }

    @Test
    fun `직전의견 없음과 빈 문자열은 같은 해시다 - KIS 공란 표기 흔들림 흡수`() {
        assertEquals(
            OpinionObservation.contentHash("매수", null, 95000),
            OpinionObservation.contentHash("매수", "", 95000),
        )
    }

    @Test
    fun `source_key 형식`() {
        val o = observation()
        assertEquals("opinion:005930:20260807:00005:${o.contentHash}", o.sourceKey)
    }

    @Test
    fun `제목은 회원사명, 없으면 코드 폴백`() {
        assertEquals("미래에셋 투자의견 매수", observation().title)
        assertEquals("00005 투자의견 매수", observation(brokerName = null).title)
    }

    @Test
    fun `payload 매핑 - category report·kind opinion·occurredAt은 수집 시각`() {
        val data = observation().toStreamData()
        assertEquals("report", data.category)
        assertEquals(OpinionData.KIND, data.kind)
        assertEquals(1785106800000, data.occurredAt)
        assertNull(data.summary)
        assertNull(data.sentiment)
        val opinion = requireNotNull(data.opinion)
        assertEquals("00005", opinion.brokerCode)
        assertEquals("미래에셋", opinion.brokerName)
        assertEquals("매수", opinion.rating)
        assertEquals("중립", opinion.previousRating)
        assertEquals(95000, opinion.targetPrice)
        assertEquals("20260807", opinion.businessDate)
    }
}
