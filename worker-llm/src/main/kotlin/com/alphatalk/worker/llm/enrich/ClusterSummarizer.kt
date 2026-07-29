package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.Sentiment
import org.springframework.stereotype.Service

@Service
class ClusterSummarizer(
    private val llm: LlmClient,
    private val confidenceFloor: Double = 0.6,
) {
    fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val raw = llm.summarize(input)
        return raw.copy(
            stocks = raw.stocks.map { it.applyFloor() },
            sectors = raw.sectors.map { it.applyFloor() },
        )
    }

    private fun StockVerdict.applyFloor(): StockVerdict =
        if (confidence < confidenceFloor) copy(sentiment = Sentiment.NEUTRAL) else this

    private fun SectorVerdict.applyFloor(): SectorVerdict =
        if (confidence < confidenceFloor) copy(sentiment = Sentiment.NEUTRAL) else this
}
