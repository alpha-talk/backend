package com.alphatalk.worker.ingest.config

import com.alphatalk.worker.ingest.source.NaverSearchNewsSource
import com.alphatalk.worker.ingest.source.NewsSource
import com.alphatalk.worker.ingest.source.RssFeedClient
import com.alphatalk.worker.ingest.source.RssNewsSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Configuration
class IngestConfig {

    @Bean(FETCH_EXECUTOR_BEAN, destroyMethod = "shutdown")
    fun ingestFetchExecutor(props: IngestProperties): ExecutorService =
        Executors.newFixedThreadPool(
            props.fetchConcurrency,
            Thread.ofPlatform().name("ingest-fetch-", 0).factory(),
        )

    @Bean(CATCH_UP_EXECUTOR_BEAN, destroyMethod = "shutdown")
    fun digestCatchUpExecutor(): ExecutorService =
        Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("digest-catchup-", 0).factory())

    @Bean
    fun newsSources(props: IngestProperties, rssFeedClient: RssFeedClient): List<NewsSource> {
        val rss = props.feeds.map { RssNewsSource(it.id, it.source, it.url, rssFeedClient) }
        if (props.naver.clientId.isBlank()) return rss
        val naver = NaverSearchNewsSource(
            clientId = props.naver.clientId,
            clientSecret = props.naver.clientSecret,
            queries = props.stocks.mapNotNull { stock ->
                stock.name.takeIf { it.isNotBlank() }?.let { NaverSearchNewsSource.StockQuery(stock.code, it) }
            },
            display = props.naver.display,
        )
        return rss + naver
    }

    companion object {
        const val FETCH_EXECUTOR_BEAN = "ingestFetchExecutor"
        const val CATCH_UP_EXECUTOR_BEAN = "digestCatchUpExecutor"
    }
}
