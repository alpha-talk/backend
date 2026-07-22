package com.alphatalk.worker.ingest.mapping

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class DictionaryStockCodeMapperTest {
    private val mapper = DictionaryStockCodeMapper(
        stocks = mapOf(
            "005930" to listOf("삼성전자", "삼전"),
            "000660" to listOf("SK하이닉스", "하이닉스"),
        ),
        macroKeywords = listOf("금리", "환율"),
    )

    @Test
    fun `종목명 매칭 - 다중 종목`() {
        val result = mapper.map("삼성전자·하이닉스 동반 강세", null)
        assertEquals(listOf("000660", "005930"), result.codes)
        assertNull(result.macroHint)
    }

    @Test
    fun `별칭 매칭 - 발췌 본문도 검색`() {
        val result = mapper.map("반도체 업황", "삼전 실적 개선 전망")
        assertEquals(listOf("005930"), result.codes)
    }

    @Test
    fun `종목 매칭 시 매크로 힌트는 붙이지 않는다`() {
        val result = mapper.map("금리 인상에도 삼성전자 강세", null)
        assertEquals(listOf("005930"), result.codes)
        assertNull(result.macroHint)
    }

    @Test
    fun `종목 미매칭 + 매크로 키워드 - 힌트만`() {
        val result = mapper.map("한은, 기준금리 25bp 인상", null)
        assertEquals(emptyList(), result.codes)
        assertEquals("금리", result.macroHint)
    }

    @Test
    fun `전부 미매칭 - unmatched`() {
        assertTrue(mapper.map("오늘의 날씨", null).unmatched)
    }
}
