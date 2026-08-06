package com.alphatalk.worker.llm.enrich

import kotlin.test.Test
import kotlin.test.assertEquals
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

    private fun Map<String, Any>.properties(name: String): Map<*, *> =
        (this["properties"] as Map<*, *>)[name] as Map<*, *>
}
