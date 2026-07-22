package com.alphatalk.worker.llm.config

import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.article.ArticleRequestGate
import com.alphatalk.worker.llm.article.ArticleUrlPolicy
import com.alphatalk.worker.llm.article.JsoupArticleFetcher
import com.alphatalk.worker.llm.article.RedisArticleRequestGate
import com.alphatalk.worker.llm.article.RobotsPolicy
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
import com.alphatalk.worker.llm.enrich.SpringTransactionRunner
import com.alphatalk.worker.llm.enrich.TransactionRunner
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
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
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
    fun embeddingClient(props: LlmProperties): EmbeddingClient = when {
        props.embedding.provider == "rest" -> {
            val e = props.embedding
            check(e.baseUrl.isNotBlank() && e.apiKey.isNotBlank() && e.model.isNotBlank()) {
                "embedding provider=rest에는 base-url·api-key·model이 모두 필요하다"
            }
            RestEmbeddingClient(e)
        }
        props.allowFake -> {
            log.info("using fake embedding client (provider={}, allow-fake)", props.embedding.provider)
            FakeEmbeddingClient(props.embedding.dimension)
        }
        else -> throw IllegalStateException(
            "임베딩 미구성 — provider=rest(base-url·api-key·model) 설정 필수. 로컬·테스트는 alphatalk.llm.allow-fake=true",
        )
    }

    @Bean
    fun llmClient(props: LlmProperties, meters: MeterRegistry): LlmClient = when {
        props.anthropic.apiKey.isNotBlank() -> AnthropicLlmClient(props, meters)
        props.allowFake -> {
            log.info("ANTHROPIC_API_KEY 미설정 + allow-fake — 규칙 기반 fake LLM으로 동작")
            FakeLlmClient()
        }
        else -> throw IllegalStateException(
            "ANTHROPIC_API_KEY 미설정 — fail-closed. 로컬·테스트는 alphatalk.llm.allow-fake=true",
        )
    }

    @Bean
    fun articleRequestGate(redis: StringRedisTemplate, props: LlmProperties): ArticleRequestGate =
        RedisArticleRequestGate(redis, props.article.minHostInterval)

    @Bean
    fun robotsPolicy(gate: ArticleRequestGate): RobotsPolicy = RobotsPolicy(gate = gate)

    @Bean
    fun articleFetcher(props: LlmProperties, robots: RobotsPolicy, gate: ArticleRequestGate): ArticleFetcher {
        val policy = ArticleUrlPolicy(props.article.allowedHostSuffixes)
        if (!policy.enabled) {
            log.info("article body fetch 비활성 — allowed-host-suffixes 미설정(제목·발췌만 사용)")
        }
        return JsoupArticleFetcher(policy, robots, gate)
    }

    @Bean
    fun eventIdGenerator(): EventIdGenerator = UlidEventIdGenerator()

    @Bean
    fun streamPublisher(redis: StringRedisTemplate, mapper: ObjectMapper): StreamPublisher =
        RedisStreamPublisher(redis, mapper)

    @Bean
    fun transactionRunner(manager: PlatformTransactionManager): TransactionRunner =
        SpringTransactionRunner(TransactionTemplate(manager))

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
        transactions: TransactionRunner,
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
        transactions = transactions,
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
    fun consumerLifecycle(consumer: IngestConsumer, props: LlmProperties, meters: MeterRegistry): ConsumerLifecycle {
        val pending = AtomicLong(0)
        Gauge.builder("queue.ingest.pending", pending, AtomicLong::toDouble)
            .description("queue:ingest 소비자 그룹 g:llm PEL 크기 (기획안 §10 알람 대상)")
            .register(meters)
        return ConsumerLifecycle(consumer, props.claimInterval, pending)
    }
}
