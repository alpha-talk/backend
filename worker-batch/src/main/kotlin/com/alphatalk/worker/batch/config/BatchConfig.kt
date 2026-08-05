package com.alphatalk.worker.batch.config

import com.alphatalk.kis.master.KisMasterClient
import com.alphatalk.worker.batch.industry.HttpDartClient
import com.alphatalk.worker.batch.industry.IndustryStore
import com.alphatalk.worker.batch.industry.IndustrySyncJob
import com.alphatalk.worker.batch.industry.KsicCatalog
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.master.MasterFileFetcher
import com.alphatalk.worker.batch.master.SectorStore
import com.alphatalk.worker.batch.master.StockMasterStore
import com.alphatalk.worker.batch.master.StockMasterSyncJob
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
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
    @ConditionalOnExpression("\${alphatalk.batch.dart.enabled:true} and !'\${alphatalk.batch.dart.api-key:}'.isEmpty()")
    fun industrySyncJob(
        props: BatchProperties,
        ksic: KsicCatalog,
        store: IndustryStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
        mapper: ObjectMapper,
    ): IndustrySyncJob {
        val client = HttpDartClient(props.dart.apiKey, props.dart.baseUrl, mapper)
        return IndustrySyncJob(
            dart = client,
            ksic = ksic,
            store = store,
            runs = runs,
            meters = meters,
            requestInterval = props.dart.requestInterval,
            groupMaxSize = props.dart.groupMaxSize,
            groupOverrides = props.dart.groupOverrides,
        )
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
