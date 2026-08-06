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
    val stocks: List<Stock> = emptyList(),
    val naver: Naver = Naver(),
    val digest: Digest = Digest(),
) {
    data class Feed(
        val id: String,
        val source: String,
        val url: String,
    )

    data class Stock(val code: String, val name: String)

    data class Naver(
        val clientId: String = "",
        val clientSecret: String = "",
        val display: Int = 30,
    )

    data class Digest(
        val enabled: Boolean = true,
        val cron: String = "0 0 18 * * *",
        val zone: String = "Asia/Seoul",
        val catchUpOnStartup: Boolean = true,
        val catchUpReconcileDelay: Duration = Duration.ofMinutes(1),
        val marketEnabled: Boolean = false,
        val marketCron: String = "0 40 17 * * *",
    )
}
