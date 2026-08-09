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
) {
    data class StockMaster(
        val enabled: Boolean = true,
        val baseUrl: String = KisMasterClient.DEFAULT_BASE_URL,
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
