package com.alphatalk.worker.llm.enrich

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class StructuredLlmCodecTest {

    @Test
    fun `요약 프롬프트는 impact 판정 기준을 담는다`() {
        val prompt = StructuredLlmCodec.summaryPrompt(
            ClusterSummaryInput(
                repTitle = "기준금리 인상",
                articleTitles = listOf("기준금리 인상"),
                body = null,
                stocks = emptyList(),
                sectors = listOf(SectorCandidate("641", "은행")),
            ),
        )

        assertTrue(prompt.contains(StructuredLlmCodec.IMPACT_CRITERIA), "impact 기준이 프롬프트에서 빠졌다")
        listOf("HIGH", "MEDIUM", "LOW").forEach {
            assertTrue(StructuredLlmCodec.IMPACT_CRITERIA.contains(it), "$it 기준이 없다")
        }
    }

    @Test
    fun `섹터 스키마의 impact에도 같은 기준을 description으로 싣는다`() {
        val sectors = StructuredLlmCodec.summarySchema.properties("sectors")
        val impact = (sectors["items"] as Map<*, *>).let { it["properties"] as Map<*, *> }["impact"] as Map<*, *>

        assertEquals(StructuredLlmCodec.IMPACT_CRITERIA, impact["description"])
        assertEquals(listOf("HIGH", "MEDIUM", "LOW"), impact["enum"])
    }

    @Test
    fun `시장 다이제스트 파싱 - 출처 참조가 온전하면 통과한다`() {
        val output = StructuredLlmCodec.parseMarketDigest(
            StructuredLlmCodec.mapper.readTree(
                """
                {"summary":"s","domestic":[{"title":"순환매","line":"l"}],
                 "global":[{"title":"미 금리","line":"l","sourceIds":["s1"]}],
                 "sources":[{"id":"s1","title":"t","url":"https://e.com","publisher":"Reuters"}]}
                """,
            ),
        )

        assertEquals(listOf("s1"), output.global.single().sourceIds)
        assertEquals("Reuters", output.sources.single().publisher)
    }

    @Test
    fun `시장 다이제스트 파싱 - 실존하지 않는 출처를 가리키면 거부한다`() {
        assertFailsWith<IllegalStateException> {
            StructuredLlmCodec.parseMarketDigest(
                StructuredLlmCodec.mapper.readTree(
                    """
                    {"summary":"s","domestic":[],
                     "global":[{"title":"미 금리","line":"l","sourceIds":["ghost"]}],
                     "sources":[{"id":"s1","title":"t","url":"https://e.com"}]}
                    """,
                ),
            )
        }
    }

    @Test
    fun `시장 다이제스트 파싱 - http가 아닌 출처 url은 거부한다`() {
        assertFailsWith<IllegalStateException> {
            StructuredLlmCodec.parseMarketDigest(
                StructuredLlmCodec.mapper.readTree(
                    """
                    {"summary":"s","domestic":[],
                     "global":[{"title":"미 금리","line":"l","sourceIds":["s1"]}],
                     "sources":[{"id":"s1","title":"t","url":"javascript:alert(1)"}]}
                    """,
                ),
            )
        }
    }

    @Test
    fun `시장 다이제스트 파싱 - 출처 id가 중복이면 거부한다`() {
        assertFailsWith<IllegalStateException> {
            StructuredLlmCodec.parseMarketDigest(
                StructuredLlmCodec.mapper.readTree(
                    """
                    {"summary":"s","domestic":[],"global":[],
                     "sources":[{"id":"s1","title":"t","url":"https://e.com"},{"id":"s1","title":"t2","url":"https://e2.com"}]}
                    """,
                ),
            )
        }
    }

    @Test
    fun `시장 다이제스트 프롬프트 - 리서치 여부에 따라 검색 지시가 갈린다`() {
        val input = MarketDigestInput(
            date = "2026-07-16",
            factSheet = null,
            marketClusters = emptyList(),
            sectorClusters = emptyList(),
            research = true,
        )

        assertTrue(StructuredLlmCodec.marketDigestPrompt(input).contains("웹 검색으로"))
        assertTrue(StructuredLlmCodec.marketDigestPrompt(input.copy(research = false)).contains("웹 검색 없이"))
    }

    private fun Map<String, Any>.properties(name: String): Map<*, *> =
        (this["properties"] as Map<*, *>)[name] as Map<*, *>
}
