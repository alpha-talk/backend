package com.alphatalk.worker.ingest.config

import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.mapping.DictionaryStockCodeMapper
import com.alphatalk.worker.ingest.mapping.StockCodeMapper
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.source.NaverSearchNewsSource
import com.alphatalk.worker.ingest.source.NewsSource
import com.alphatalk.worker.ingest.source.RssNewsSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class IngestConfig {

    @Bean(destroyMethod = "shutdown")
    fun ingestFetchExecutor(props: IngestProperties): ExecutorService =
        Executors.newFixedThreadPool(
            props.fetchConcurrency,
            Thread.ofPlatform().name("ingest-fetch-", 0).factory(),
        )

    @Bean
    fun stockCodeMapper(props: IngestProperties): StockCodeMapper =
        DictionaryStockCodeMapper(
            stocks = props.stocks.associate { it.code to it.names },
            macroKeywords = props.macroKeywords,
        )

    @Bean
    fun ingestPoller(
        mapper: StockCodeMapper,
        seen: SeenMarker,
        queue: IngestQueue,
        ingestFetchExecutor: ExecutorService,
        props: IngestProperties,
    ): IngestPoller = IngestPoller(
        sources = newsSources(props),
        mapper = mapper,
        seen = seen,
        queue = queue,
        excerptMaxLength = props.excerptMaxLength,
        fetchExecutor = ingestFetchExecutor,
    )

    private fun newsSources(props: IngestProperties): List<NewsSource> {
        val rss = props.feeds.map { RssNewsSource(it.name, it.url) }
        if (props.naver.clientId.isBlank()) return rss
        val naver = NaverSearchNewsSource(
            clientId = props.naver.clientId,
            clientSecret = props.naver.clientSecret,
            queries = props.stocks.mapNotNull { stock ->
                stock.names.firstOrNull()?.let { NaverSearchNewsSource.StockQuery(stock.code, it) }
            },
            display = props.naver.display,
        )
        return rss + naver
    }
}
