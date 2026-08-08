package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ClusterSummarizerTest {
    private fun fixedLlm(output: ClusterSummaryOutput) = object : LlmClient {
        override fun summarize(input: ClusterSummaryInput) = output
        override fun digest(input: DigestInput) = DigestOutput("t", "s")
        override fun marketDigest(input: MarketDigestInput) =
            MarketDigestOutput("m", emptyList(), emptyList(), emptyList())
    }

    private val input = ClusterSummaryInput("제목", listOf("제목"), null, emptyList(), emptyList())

    @Test
    fun `확신도 미달 감성은 NEUTRAL로 강등`() {
        val summarizer = ClusterSummarizer(
            fixedLlm(
                ClusterSummaryOutput(
                    summary = "요약",
                    marketRelevant = true,
                    scope = NewsScope.STOCK,
                    stocks = listOf(
                        StockVerdict("005930", true, Sentiment.POSITIVE, 0.55, "낮음"),
                        StockVerdict("000660", true, Sentiment.NEGATIVE, 0.9, "높음"),
                    ),
                    sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.3, "낮음")),
                ),
            ),
        )
        val out = summarizer.summarize(input)
        assertEquals(Sentiment.NEUTRAL, out.stocks[0].sentiment)
        assertEquals(Sentiment.NEGATIVE, out.stocks[1].sentiment)
        assertEquals(Sentiment.NEUTRAL, out.sectors[0].sentiment)
    }
}
