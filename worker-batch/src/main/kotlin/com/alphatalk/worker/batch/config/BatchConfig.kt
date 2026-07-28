package com.alphatalk.worker.batch.config

import com.alphatalk.kis.master.KisMasterClient
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.master.MasterFileFetcher
import com.alphatalk.worker.batch.master.SectorStore
import com.alphatalk.worker.batch.master.StockMasterStore
import com.alphatalk.worker.batch.master.StockMasterSyncJob
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class BatchConfig {
    @Bean
    fun masterFileFetcher(props: BatchProperties): MasterFileFetcher {
        val client = KisMasterClient(props.stockMaster.baseUrl)
        return object : MasterFileFetcher {
            override fun fetch(market: com.alphatalk.kis.master.KisMarket) = client.download(market)
            override fun fetchSectors() = client.downloadSectors()
        }
    }

    @Bean
    @ConditionalOnProperty("alphatalk.batch.stock-master.enabled", havingValue = "true", matchIfMissing = true)
    fun stockMasterSyncJob(
        files: MasterFileFetcher,
        stocks: StockMasterStore,
        sectors: SectorStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
    ): StockMasterSyncJob = StockMasterSyncJob(files, stocks, sectors, runs, meters)
}
