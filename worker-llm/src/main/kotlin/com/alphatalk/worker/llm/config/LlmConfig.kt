package com.alphatalk.worker.llm.config

import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.article.JsoupArticleFetcher
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterLock
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.EmbeddingClient
import com.alphatalk.worker.llm.cluster.FakeEmbeddingClient
import com.alphatalk.worker.llm.cluster.JdbcClusterStore
import com.alphatalk.worker.llm.cluster.RedisClusterLock
import com.alphatalk.worker.llm.cluster.RestEmbeddingClient
import com.alphatalk.worker.llm.consume.ConsumerLifecycle
import com.alphatalk.worker.llm.consume.IngestConsumer
import com.alphatalk.worker.llm.enrich.AnthropicLlmClient
import com.alphatalk.worker.llm.enrich.ClusterSummarizer
import com.alphatalk.worker.llm.enrich.DigestProcessor
import com.alphatalk.worker.llm.enrich.FakeLlmClient
import com.alphatalk.worker.llm.enrich.LlmClient
import com.alphatalk.worker.llm.enrich.NewsProcessor
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.JdbcStreamEventStore
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.persist.UlidEventIdGenerator
import com.alphatalk.worker.llm.publish.RedisStreamPublisher
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.JdbcSectorDirectory
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.f4b6a3.ulid.UlidCreator
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.time.Duration

@Configuration
class LlmConfig {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun clusterStore(jdbc: NamedParameterJdbcTemplate): ClusterStore = JdbcClusterStore(jdbc)

    @Bean
    fun streamEventStore(jdbc: NamedParameterJdbcTemplate, mapper: ObjectMapper): StreamEventStore =
        JdbcStreamEventStore(jdbc, mapper)

    @Bean
    fun sectorDirectory(jdbc: NamedParameterJdbcTemplate): SectorDirectory = JdbcSectorDirectory(jdbc)

    @Bean
    fun clusterLock(redis: StringRedisTemplate, props: LlmProperties): ClusterLock =
        RedisClusterLock(redis, props.cluster.lockTtl)

    @Bean
    fun embeddingClient(props: LlmProperties): EmbeddingClient =
        if (props.embedding.provider == "rest" && props.embedding.baseUrl.isNotBlank()) {
            RestEmbeddingClient(props.embedding)
        } else {
            log.info("using fake embedding client (provider={})", props.embedding.provider)
            FakeEmbeddingClient(props.embedding.dimension)
        }

    @Bean
    fun llmClient(props: LlmProperties, meters: MeterRegistry): LlmClient =
        if (props.anthropic.apiKey.isNotBlank()) {
            AnthropicLlmClient(props, meters)
        } else {
            log.info("ANTHROPIC_API_KEY 미설정 — 규칙 기반 fake LLM으로 동작")
            FakeLlmClient()
        }

    @Bean
    fun articleFetcher(): ArticleFetcher = JsoupArticleFetcher()

    @Bean
    fun eventIdGenerator(): EventIdGenerator = UlidEventIdGenerator()

    @Bean
    fun streamPublisher(redis: StringRedisTemplate, mapper: ObjectMapper): StreamPublisher =
        RedisStreamPublisher(redis, mapper)

    @Bean
    fun clusterAssigner(
        store: ClusterStore,
        embeddings: EmbeddingClient,
        lock: ClusterLock,
        props: LlmProperties,
    ): ClusterAssigner = ClusterAssigner(
        store = store,
        embeddings = embeddings,
        lock = lock,
        window = Duration.ofHours(props.cluster.windowHours),
        similarityThreshold = props.cluster.similarityThreshold,
        clusterIds = { UlidCreator.getMonotonicUlid().toString() },
    )

    @Bean
    fun clusterSummarizer(llm: LlmClient): ClusterSummarizer = ClusterSummarizer(llm)

    @Bean
    fun newsProcessor(
        store: ClusterStore,
        assigner: ClusterAssigner,
        fetcher: ArticleFetcher,
        summarizer: ClusterSummarizer,
        sectors: SectorDirectory,
        events: StreamEventStore,
        publisher: StreamPublisher,
        eventIds: EventIdGenerator,
        mapper: ObjectMapper,
        meters: MeterRegistry,
        props: LlmProperties,
    ): NewsProcessor = NewsProcessor(
        store = store,
        assigner = assigner,
        fetcher = fetcher,
        summarizer = summarizer,
        sectors = sectors,
        events = events,
        publisher = publisher,
        eventIds = eventIds,
        mapper = mapper,
        meters = meters,
        fanoutCap = props.sector.fanoutCap,
        coverageStocks = props.sector.coverageStocks,
    )

    @Bean
    fun digestProcessor(
        store: ClusterStore,
        sectors: SectorDirectory,
        events: StreamEventStore,
        publisher: StreamPublisher,
        eventIds: EventIdGenerator,
        llm: LlmClient,
    ): DigestProcessor = DigestProcessor(
        store = store,
        sectors = sectors,
        events = events,
        publisher = publisher,
        eventIds = eventIds,
        llm = llm,
    )

    @Bean
    fun ingestConsumer(
        redis: StringRedisTemplate,
        news: NewsProcessor,
        digest: DigestProcessor,
        meters: MeterRegistry,
        props: LlmProperties,
    ): IngestConsumer = IngestConsumer(
        redis = redis,
        news = news,
        digest = digest,
        meters = meters,
        block = props.consumerBlock,
        batch = props.consumerBatch,
        poisonMaxDeliveries = props.poisonMaxDeliveries,
        claimIdle = props.claimIdle,
    )

    @Bean
    @ConditionalOnProperty("alphatalk.llm.consume-enabled", havingValue = "true", matchIfMissing = true)
    fun consumerLifecycle(consumer: IngestConsumer, props: LlmProperties): ConsumerLifecycle =
        ConsumerLifecycle(consumer, props.claimInterval)
}
