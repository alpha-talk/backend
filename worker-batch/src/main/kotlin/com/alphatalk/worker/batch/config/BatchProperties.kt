package com.alphatalk.worker.batch.config

import com.alphatalk.kis.master.KisMasterClient
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("alphatalk.batch")
data class BatchProperties(
    val enabled: Boolean = true,
    val holidays: List<String> = emptyList(),
    val stockMaster: StockMaster = StockMaster(),
    val dart: Dart = Dart(),
    val kis: Kis = Kis(),
    val opinion: Opinion = Opinion(),
    val valuation: Valuation = Valuation(),
    val investor: Investor = Investor(),
    val financials: Financials = Financials(),
    val catchUp: CatchUp = CatchUp(),
) {
    data class StockMaster(
        val enabled: Boolean = true,
        val baseUrl: String = KisMasterClient.DEFAULT_BASE_URL,
        val cron: String = "0 0 8 * * *",
        val retryCron: String = "0 0 9,10,12 * * *",
    )

    data class CatchUp(
        val enabled: Boolean = true,
        val passes: Int = 3,
        val passInterval: Duration = Duration.ofMinutes(20),
        val stopTimeout: Duration = Duration.ofSeconds(5),
    )

    data class Kis(
        val accountsJson: String = "[]",
        val rateFactor: Double = 0.75,
    )

    data class Opinion(
        val enabled: Boolean = false,
        val businessDayEnforced: Boolean = true,
        val callsPerSecond: Double = 4.0,
        val requestInterval: Duration = Duration.ofMillis(250),
        val scanLimit: Int = 500,
    )

    data class Valuation(
        val enabled: Boolean = false,
        val cron: String = "0 50 16 * * MON-FRI",
        val callsPerSecond: Double = 8.0,
        val chunkSize: Int = 200,
    )

    data class Investor(
        val enabled: Boolean = false,
        val cron: String = "0 10 17 * * MON-FRI",
        val retryCron: String = "0 40 17,18 * * MON-FRI",
        val callsPerSecond: Double = 8.0,
    )

    data class Financials(
        val enabled: Boolean = false,
        val cron: String = "0 0 6 * * *",
        val retryCron: String = "0 0 9,13,17 * * *",
        val lookbackDays: Long = 7,
        val backfillYears: Long = 3,
        val backfillPerRun: Int = 100,
        val failureStreakLimit: Int = 5,
    )

    data class Dart(
        val enabled: Boolean = true,
        val apiKey: String = "",
        val baseUrl: String = "https://opendart.fss.or.kr/api",
        val requestInterval: Duration = Duration.ofMillis(50),
        val groupMaxSize: Int = 100,
        val groupOverrides: Map<String, String> = emptyMap(),
        val maxFailureRatio: Double = 0.05,
        val failureStreakLimit: Int = 5,
        val deadline: Duration = Duration.ofMinutes(90),
    )
}
