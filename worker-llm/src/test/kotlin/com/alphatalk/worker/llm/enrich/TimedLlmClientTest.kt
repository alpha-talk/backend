package com.alphatalk.worker.llm.enrich

import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class TimedLlmClientTest {
    private val meters = SimpleMeterRegistry()

    @Test
    fun `성공한 호출은 op별 success 타이머에 기록된다`() {
        val client = TimedLlmClient(FakeLlmClient(), meters)

        client.summarize(ClusterSummaryInput("제목", listOf("제목"), null, emptyList(), emptyList()))
        client.digest(DigestInput("005930", "삼성전자", "2026-08-29", emptyList(), emptyList(), emptyList()))

        assertEquals(1, timerCount("summarize", "success"))
        assertEquals(1, timerCount("digest", "success"))
        assertNull(meters.find("llm.call").tag("outcome", "error").timer())
    }

    @Test
    fun `실패한 호출은 error 타이머에 기록되고 예외는 그대로 전파된다`() {
        val failing = object : LlmClient {
            override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput = throw IllegalStateException("boom")
            override fun digest(input: DigestInput): DigestOutput = throw IllegalStateException("boom")
            override fun marketDigest(input: MarketDigestInput): MarketDigestOutput = throw IllegalStateException("boom")
        }
        val client = TimedLlmClient(failing, meters)

        assertFailsWith<IllegalStateException> {
            client.marketDigest(MarketDigestInput("2026-08-29", "2026-08-29T18:00:00+09:00", null, emptyList(), emptyList(), research = false))
        }

        assertEquals(1, timerCount("market_digest", "error"))
        assertNull(meters.find("llm.call").tag("outcome", "success").timer())
    }

    @Test
    fun `리서치 지원 여부는 위임한다`() {
        val researchCapable = object : LlmClient by FakeLlmClient() {
            override fun supportsMarketResearch(): Boolean = true
        }

        assertTrue(TimedLlmClient(researchCapable, meters).supportsMarketResearch())
    }

    private fun timerCount(op: String, outcome: String): Long =
        meters.find("llm.call").tag("op", op).tag("outcome", outcome).timer()?.count() ?: 0
}
