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
    val digest: Digest = Digest(),
) {
    data class Feed(
        val id: String,
        val source: String,
        val url: String,
    )

    data class Digest(
        val enabled: Boolean = true,
        val cron: String = "0 0 18 * * *",
        val zone: String = "Asia/Seoul",
        val catchUpOnStartup: Boolean = true,
        val catchUpReconcileDelay: Duration = Duration.ofMinutes(1),
        val marketEnabled: Boolean = true,
        val marketCron: String = "0 40 17 * * *",
        val enqueueConcurrency: Int = 1,
    ) {
        init {
            require(enqueueConcurrency >= 1) {
                "alphatalk.ingest.digest.enqueue-concurrency는 1 이상이어야 한다: $enqueueConcurrency"
            }
        }
    }
}
