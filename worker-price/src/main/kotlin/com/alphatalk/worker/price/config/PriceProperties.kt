package com.alphatalk.worker.price.config

import org.springframework.boot.context.properties.ConfigurationProperties

enum class DemandMode { FIXED, REDIS }

@ConfigurationProperties("alphatalk.price")
data class PriceProperties(
    val enabled: Boolean = false,
    val env: String = "vts",
    val accountsJson: String = "[]",
    val symbols: List<String> = emptyList(),
    val demandMode: DemandMode = DemandMode.FIXED,
    val conflationMs: Long = 200,
    val rateFactor: Double = 0.75,
    val maintainIntervalMs: Long = 1_000,
    val removalGraceMs: Long = 30_000,
    val marketHoursEnforced: Boolean = true,
    val holidays: List<String> = emptyList(),
    val pollIntervalMs: Long = 30_000,
    val pollBudgetFactor: Double = 0.5,
    val candleEnabled: Boolean = false,
    val candleBackfillDays: Long = 90,
    val candleSyncOnStartup: Boolean = false,
)
