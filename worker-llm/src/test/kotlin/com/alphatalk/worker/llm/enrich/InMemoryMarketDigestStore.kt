package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.MarketAnalysis
import com.alphatalk.worker.llm.persist.MarketDigestStore

class InMemoryMarketDigestStore : MarketDigestStore {
    val saved = mutableMapOf<String, MarketAnalysis>()
    var failing = false

    override fun find(date: String): MarketAnalysis? {
        if (failing) throw IllegalStateException("db down")
        return saved[date]
    }

    override fun save(date: String, analysis: MarketAnalysis): Boolean {
        if (failing) throw IllegalStateException("db down")
        val existing = saved[date]
        if (existing != null && !(existing.degraded && !analysis.degraded)) return false
        saved[date] = analysis
        return true
    }
}
