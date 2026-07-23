package com.alphatalk.worker.llm.config

import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.article.ArticleUrlPolicy
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.EmbeddingClient
import com.alphatalk.worker.llm.cluster.FakeEmbeddingClient
import com.alphatalk.worker.llm.cluster.ClusterLock
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.RestEmbeddingClient
import com.alphatalk.worker.llm.enrich.AnthropicLlmClient
import com.alphatalk.worker.llm.enrich.ClusterSummarizer
import com.alphatalk.worker.llm.enrich.FakeLlmClient
import com.alphatalk.worker.llm.enrich.LlmClient
import com.alphatalk.worker.llm.enrich.NewsProcessor
import com.alphatalk.worker.llm.enrich.TransactionRunner
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.f4b6a3.ulid.UlidCreator
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class LlmConfig {
    private val log = LoggerFactory.getLogger(javaClass)

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
    fun articleUrlPolicy(props: LlmProperties): ArticleUrlPolicy =
        ArticleUrlPolicy(props.article.allowedHostSuffixes)

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
        fanoutCap = props.sector.fanoutCap,
        coverageStocks = props.sector.coverageStocks,
        transactions = transactions,
    )
}
