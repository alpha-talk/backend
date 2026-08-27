package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.MarketAnalysis
import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment

class FakeLlmClient : LlmClient {

    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val text = (input.articleTitles + listOfNotNull(input.body)).joinToString(" ")
        val sentiment = sentimentOf(text)
        val relevantStocks = input.stocks.filter { text.contains(it.name) || text.contains(it.code) }
        val matchedSectors = input.sectors.filter { text.contains(it.name) }
        val marketRelevant = relevantStocks.isNotEmpty() ||
            matchedSectors.isNotEmpty() ||
            MARKET_KEYWORDS.any(text::contains)
        val scope = when {
            relevantStocks.isNotEmpty() -> NewsScope.STOCK
            matchedSectors.isNotEmpty() -> NewsScope.SECTOR
            else -> NewsScope.MARKET
        }
        return ClusterSummaryOutput(
            summary = summaryOf(input),
            marketRelevant = marketRelevant,
            scope = scope,
            stocks = input.stocks.map {
                StockVerdict(
                    code = it.code,
                    relevant = it in relevantStocks,
                    sentiment = sentiment,
                    confidence = if (sentiment == Sentiment.NEUTRAL) 0.5 else 0.9,
                    reason = "keyword",
                    relation = StockRelation.DIRECT,
                    evidence = it.name,
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

    override fun marketDigest(input: MarketDigestInput): MarketDigestOutput {
        val domestic = buildList {
            input.factSheet?.topSectors?.firstOrNull()?.let {
                add(
                    MarketAnalysis.DomesticItem(
                        title = "${it.name} 강세",
                        line = "업종 평균 ${"%.2f".format(it.avgChangePct)}% (${it.stockCount}종목)",
                    ),
                )
            }
            (input.marketClusters + input.sectorClusters).take(2).forEach {
                add(MarketAnalysis.DomesticItem(title = it.title, line = firstLine(it.summary)))
            }
        }
        return MarketDigestOutput(
            summary = domestic.joinToString("\n") { it.title }.ifBlank { "오늘의 시장 소식이 없습니다" },
            domestic = domestic,
            global = emptyList(),
            sources = emptyList(),
        )
    }

    private fun firstLine(summary: String): String =
        summary.lineSequence().firstOrNull().orEmpty().take(80)

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
        private val MARKET_KEYWORDS =
            listOf("증시", "코스피", "코스닥", "금리", "환율", "유가", "물가", "수출", "관세", "경기")
    }
}
