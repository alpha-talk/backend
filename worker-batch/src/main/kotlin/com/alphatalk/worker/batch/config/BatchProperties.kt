package com.alphatalk.worker.batch.config

import com.alphatalk.kis.master.KisMasterClient
import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("alphatalk.batch")
data class BatchProperties(
    val enabled: Boolean = true,
    val stockMaster: StockMaster = StockMaster(),
    val dart: Dart = Dart(),
) {
    data class StockMaster(
        val enabled: Boolean = true,
        val baseUrl: String = KisMasterClient.DEFAULT_BASE_URL,
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
