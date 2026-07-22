package com.alphatalk.worker.ingest.config

import com.alphatalk.worker.ingest.dedup.RedisSeenMarker
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.mapping.DictionaryStockCodeMapper
import com.alphatalk.worker.ingest.mapping.StockCodeMapper
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.queue.RedisIngestQueue
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.source.RssNewsSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate

@Configuration
class IngestConfig {

    @Bean
    fun stockCodeMapper(props: IngestProperties): StockCodeMapper =
        DictionaryStockCodeMapper(
            stocks = props.stocks.associate { it.code to it.names },
            macroKeywords = props.macroKeywords,
        )

    @Bean
    fun seenMarker(redis: StringRedisTemplate, props: IngestProperties): SeenMarker =
        RedisSeenMarker(redis, props.seenTtl)

    @Bean
    fun ingestQueue(redis: StringRedisTemplate, props: IngestProperties): IngestQueue =
        RedisIngestQueue(redis, props.queueMaxLen)

    @Bean
    fun ingestPoller(
        mapper: StockCodeMapper,
        seen: SeenMarker,
        queue: IngestQueue,
        props: IngestProperties,
    ): IngestPoller = IngestPoller(
        sources = props.feeds.map { RssNewsSource(it.name, it.url) },
        mapper = mapper,
        seen = seen,
        queue = queue,
        excerptMaxLength = props.excerptMaxLength,
    )
}
