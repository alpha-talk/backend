package com.alphatalk.worker.llm.config

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("alphatalk.llm")
data class LlmProperties(
    val consumeEnabled: Boolean = true,
    val consumerBlock: Duration = Duration.ofSeconds(5),
    val consumerBatch: Int = 8,
    val poisonMaxDeliveries: Long = 5,
    val claimIdle: Duration = Duration.ofMinutes(5),
    val claimInterval: Duration = Duration.ofMinutes(1),
    val cluster: Cluster = Cluster(),
    val sector: Sector = Sector(),
    val models: Models = Models(),
    val anthropic: Anthropic = Anthropic(),
    val embedding: Embedding = Embedding(),
) {
    data class Cluster(
        val windowHours: Long = 72,
        val similarityThreshold: Double = 0.85,
        val lockTtl: Duration = Duration.ofSeconds(3),
    )

    data class Sector(
        val fanoutCap: Int = 100,
        val coverageStocks: List<String> = emptyList(),
    )

    data class Models(
        val summary: String = "claude-haiku-4-5",
        val digest: String = "claude-sonnet-5",
    )

    data class Anthropic(
        val baseUrl: String = "https://api.anthropic.com",
        val apiKey: String = "",
        val version: String = "2023-06-01",
        val maxTokens: Int = 1024,
    )

    data class Embedding(
        val provider: String = "fake",
        val baseUrl: String = "",
        val apiKey: String = "",
        val model: String = "",
        val dimension: Int = 1024,
    )
}
