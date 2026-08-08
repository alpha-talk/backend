package com.alphatalk.worker.llm.persist

import com.alphatalk.contracts.envelope.MarketAnalysis

interface MarketDigestStore {
    fun find(date: String): MarketAnalysis?
    fun save(date: String, analysis: MarketAnalysis): Boolean
}
