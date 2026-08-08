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
    )

    data class CatchUp(
        val enabled: Boolean = true,
    )

    data class Kis(
        val accountsJson: String = "[]",
        val rateFactor: Double = 0.75,
    )

    data class Opinion(
        val enabled: Boolean = false,
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
        val callsPerSecond: Double = 8.0,
    )

    data class Financials(
        val enabled: Boolean = false,
        val cron: String = "0 0 6 * * *",
        val lookbackDays: Long = 7,
    )

    data class Dart(
        val enabled: Boolean = true,
        val apiKey: String = "",
        val baseUrl: String = "https://opendart.fss.or.kr/api",
        val requestInterval: Duration = Duration.ofMillis(50),
        val groupMaxSize: Int = 100,
        val groupOverrides: Map<String, String> = emptyMap(),
        val maxFailureRatio: Double = 0.05,
    )
}
