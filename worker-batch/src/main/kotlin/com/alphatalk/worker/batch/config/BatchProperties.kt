package com.alphatalk.worker.batch.config

import com.alphatalk.kis.master.KisMasterClient
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("alphatalk.batch")
data class BatchProperties(
    val enabled: Boolean = true,
    val stockMaster: StockMaster = StockMaster(),
) {
    data class StockMaster(
        val enabled: Boolean = true,
        val baseUrl: String = KisMasterClient.DEFAULT_BASE_URL,
    )
}
