package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment

class FakeLlmClient : LlmClient {

    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val text = (input.articleTitles + listOfNotNull(input.body)).joinToString(" ")
        val sentiment = sentimentOf(text)
        val relevantStocks = input.stocks.filter { text.contains(it.name) || text.contains(it.code) }
        val matchedSectors = input.sectors.filter { text.contains(it.name) }
        val scope = when {
            relevantStocks.isNotEmpty() -> NewsScope.STOCK
            matchedSectors.isNotEmpty() -> NewsScope.SECTOR
            else -> NewsScope.MARKET
        }
        return ClusterSummaryOutput(
            summary = summaryOf(input),
            scope = scope,
            stocks = input.stocks.map {
                StockVerdict(
                    code = it.code,
                    relevant = it in relevantStocks,
                    sentiment = sentiment,
                    confidence = if (sentiment == Sentiment.NEUTRAL) 0.5 else 0.9,
                    reason = "keyword",
                )
            },
            sectors = matchedSectors.map {
                SectorVerdict(
                    sectorCode = it.code,
                    sentiment = sentiment,
                    impact = Impact.HIGH,
                    confidence = if (sentiment == Sentiment.NEUTRAL) 0.5 else 0.9,
                    reason = "keyword",
                )
            },
        )
    }

    override fun digest(input: DigestInput): DigestOutput {
        val top = (input.stockClusters + input.sectorClusters).take(3)
        return DigestOutput(
            title = "${input.stockName} 데일리 브리핑 (${input.date})",
            summary = top.joinToString("\n") { it.title }.ifBlank { "오늘의 주요 소식이 없습니다" },
        )
    }

    private fun summaryOf(input: ClusterSummaryInput): String {
        val lead = input.body?.take(160)
        return listOfNotNull(input.repTitle, lead, "관련 기사 ${input.articleTitles.size}건")
            .joinToString("\n")
    }

    private fun sentimentOf(text: String): Sentiment = when {
        POSITIVE_KEYWORDS.any { text.contains(it) } -> Sentiment.POSITIVE
        NEGATIVE_KEYWORDS.any { text.contains(it) } -> Sentiment.NEGATIVE
        else -> Sentiment.NEUTRAL
    }

    companion object {
        private val POSITIVE_KEYWORDS =
            listOf("수주", "상승", "증가", "개선", "호재", "강세", "신고가", "흑자", "인상 수혜")
        private val NEGATIVE_KEYWORDS =
            listOf("하락", "적자", "감소", "소송", "리콜", "악재", "약세", "규제", "파산")
    }
}
