package com.alphatalk.worker.llm.enrich

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer

class TimedLlmClient(
    val delegate: LlmClient,
    private val meters: MeterRegistry,
) : LlmClient {
    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput =
        timed("summarize") { delegate.summarize(input) }

    override fun digest(input: DigestInput): DigestOutput =
        timed("digest") { delegate.digest(input) }

    override fun marketDigest(input: MarketDigestInput): MarketDigestOutput =
        timed("market_digest") { delegate.marketDigest(input) }

    override fun supportsMarketResearch(): Boolean = delegate.supportsMarketResearch()

    private fun <T> timed(op: String, call: () -> T): T {
        val sample = Timer.start(meters)
        try {
            val result = call()
            sample.stop(timer(op, "success"))
            return result
        } catch (error: Throwable) {
            sample.stop(timer(op, "error"))
            throw error
        }
    }

    private fun timer(op: String, outcome: String): Timer = Timer.builder("llm.call")
        .tag("op", op)
        .tag("outcome", outcome)
        .register(meters)
}
