package com.alphatalk.worker.ingest.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("alphatalk.ingest")
data class IngestProperties(
    val seenTtl: Duration = Duration.ofDays(7),
    val queueMaxLen: Long = 10_000,
    val excerptMaxLength: Int = 200,
    val fetchConcurrency: Int = 4,
    val feeds: List<Feed> = emptyList(),
    val macroKeywords: List<String> = emptyList(),
    val stocks: List<Stock> = emptyList(),
) {
    data class Feed(val name: String, val url: String)

    data class Stock(val code: String, val names: List<String>)
}
