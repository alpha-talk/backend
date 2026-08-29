package com.alphatalk.worker.llm

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.envelope.DigestData
import com.alphatalk.contracts.envelope.MarketAnalysis
import com.alphatalk.contracts.envelope.StreamCategory
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.article.ArticleRequestGate
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.cluster.StockVerdictWrite
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.consume.IngestConsumer
import com.alphatalk.worker.llm.enrich.ClusterSummarizer
import com.alphatalk.worker.llm.enrich.DigestProcessor
import com.alphatalk.worker.llm.enrich.MarketDigestProcessor
import com.alphatalk.worker.llm.enrich.NewsProcessor
import com.alphatalk.worker.llm.enrich.StockEvidenceValidator
import com.alphatalk.worker.llm.enrich.TransactionRunner
import com.alphatalk.worker.llm.persist.EventIdGenerator
import com.alphatalk.worker.llm.persist.JdbcMarketDigestStore
import com.alphatalk.worker.llm.persist.StreamEventRow
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.fasterxml.jackson.databind.ObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.connection.stream.StreamRecords
import org.springframework.dao.DataAccessException
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime
import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "alphatalk.llm.consumer-block=300ms",
        "alphatalk.llm.cluster.similarity-threshold=0.8",
        "alphatalk.llm.article.min-host-interval=100ms",
    ],
)
@Testcontainers(disabledWithoutDocker = true)
class LlmWorkerIntegrationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var redisTemplate: StringRedisTemplate

    @Autowired
    private lateinit var jdbc: NamedParameterJdbcTemplate

    @Autowired
    private lateinit var consumer: IngestConsumer

    @Autowired
    private lateinit var digestProcessor: DigestProcessor

    @Autowired
    private lateinit var marketDigestProcessor: MarketDigestProcessor

    @Autowired
    private lateinit var newsProcessor: NewsProcessor

    @Autowired
    private lateinit var eventStore: StreamEventStore

    @Autowired
    private lateinit var articleRequestGate: ArticleRequestGate

    @Autowired
    private lateinit var clusterStore: ClusterStore

    @Autowired
    private lateinit var transactions: TransactionRunner

    @Autowired
    private lateinit var clusterAssigner: ClusterAssigner

    @Autowired
    private lateinit var articleFetcher: ArticleFetcher

    @Autowired
    private lateinit var summarizer: ClusterSummarizer

    @Autowired
    private lateinit var sectorDirectory: SectorDirectory

    @Autowired
    private lateinit var streamPublisher: StreamPublisher

    @Autowired
    private lateinit var eventIdGenerator: EventIdGenerator

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    private fun newsProcessorWithFanoutCap(
        cap: Int,
        hardCap: Int = 500,
        meters: MeterRegistry = SimpleMeterRegistry(),
    ) = NewsProcessor(
        store = clusterStore,
        assigner = clusterAssigner,
        fetcher = articleFetcher,
        summarizer = summarizer,
        stockEvidence = StockEvidenceValidator(sectorDirectory),
        sectors = sectorDirectory,
        events = eventStore,
        publisher = streamPublisher,
        eventIds = eventIdGenerator,
        mapper = objectMapper,
        meters = meters,
        transactions = transactions,
        props = LlmProperties(sector = LlmProperties.Sector(fanoutCap = cap, fanoutHardCap = hardCap)),
    )

    @BeforeEach
    fun reset() {
        redisTemplate.execute { it.serverCommands().flushAll() }
        jdbc.jdbcTemplate.execute(
            "TRUNCATE news_article, news_cluster_stock, news_cluster_sector, news_cluster, stream_event, " +
                "stock_alias, stock_master, sector CASCADE",
        )
        jdbc.jdbcTemplate.execute(
            "INSERT INTO sector (code, name, level) VALUES ('261', '반도체', 3), ('641', '은행', 3)",
        )
        jdbc.jdbcTemplate.execute(
            """
            INSERT INTO stock_master (code, name, market, sector_code, dart_induty_code) VALUES
            ('005930', '삼성전자', 'KOSPI', '261', '26120'),
            ('000660', 'SK하이닉스', 'KOSPI', '261', '26120'),
            ('105560', 'KB금융', 'KOSPI', '641', '64110'),
            ('055550', '신한지주', 'KOSPI', '641', '64110'),
            ('086790', '하나금융지주', 'KOSPI', '641', '64110')
            """,
        )
        consumer.ensureGroup()
    }

    private fun newsEntry(
        sourceId: String,
        title: String,
        source: String = "hankyung",
        codes: List<String> = listOf("005930"),
        macroHint: String? = null,
        type: IngestType = IngestType.NEWS,
    ) = IngestQueueEntry(
        source = source,
        sourceId = sourceId,
        type = type,
        codes = codes,
        title = title,
        url = "",
        body = null,
        fetchedAt = System.currentTimeMillis(),
        macroHint = macroHint,
    )

    private fun xadd(entry: IngestQueueEntry) = xadd(entry.toFields())

    private fun xadd(fields: Map<String, String>) {
        redisTemplate.opsForStream<String, String>().add(
            StreamRecords.mapBacked<String, String, String>(fields).withStreamKey(Queues.INGEST),
        )
    }

    private fun drain(maxRounds: Int = 10) {
        repeat(maxRounds) {
            if (consumer.pollOnce() == 0) return
        }
    }

    private fun newsEventCount(code: String): Long = jdbc.queryForObject(
        "SELECT count(*) FROM stream_event WHERE code = :code AND type = 'NEWS'",
        mapOf("code" to code),
        Long::class.java,
    )!!

    @Test
    fun `ensureGroup - 그룹이 이미 있어도(BUSYGROUP) 재기동이 실패하지 않는다`() {
        consumer.ensureGroup()
    }

    @Test
    fun `FR-11 - 동일 sourceId 중복 요약 0건 + 발행 E2E`() {
        val received = LinkedBlockingQueue<String>()
        val listener = RedisMessageListenerContainer().apply {
            setConnectionFactory(redisTemplate.connectionFactory!!)
            addMessageListener({ message, _ -> received.add(String(message.body)) }, ChannelTopic(Channels.stream("005930")))
            afterPropertiesSet()
            start()
        }
        try {
            xadd(newsEntry("hankyung:a1", "삼성전자 대규모 수주"))
            drain()
            assertEquals(1, newsEventCount("005930"))

            xadd(newsEntry("hankyung:a1", "삼성전자 대규모 수주"))
            drain()
            assertEquals(1, newsEventCount("005930"))

            val message = received.poll(5, TimeUnit.SECONDS)
            assertNotNull(message)
            assertTrue(message.contains("\"POSITIVE\""))
        } finally {
            listener.destroy()
        }
    }

    @Test
    fun `N3 DoD - 동일 사건 3개 언론사 기사 - 클러스터 1·이벤트 1·sources 3`() {
        xadd(newsEntry("hankyung:b1", "[속보] 삼성전자, 3나노 수주!"))
        drain()
        xadd(newsEntry("maeil:b2", "삼성전자 3나노 수주", source = "maeil"))
        xadd(newsEntry("yonhap:b3", "삼성전자 3나노 수주 계약", source = "yonhap"))
        drain()

        val clusterCount = jdbc.queryForObject("SELECT count(*) FROM news_cluster", emptyMap<String, Any>(), Long::class.java)
        assertEquals(1, clusterCount)
        assertEquals(1, newsEventCount("005930"))
        val sourcesCount = jdbc.queryForObject(
            "SELECT jsonb_array_length(payload -> 'sources') FROM stream_event WHERE type = 'NEWS'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        assertEquals(3, sourcesCount)
    }

    @Test
    fun `V6 - report type은 클러스터와 발행 이벤트까지 보존된다`() {
        xadd(newsEntry("hankyung:report1", "삼성전자 목표주가 상향 리포트", type = IngestType.REPORT))
        drain()

        val category = jdbc.queryForObject(
            "SELECT category FROM news_cluster",
            emptyMap<String, Any>(),
            String::class.java,
        )
        val eventType = jdbc.queryForObject(
            "SELECT type FROM stream_event",
            emptyMap<String, Any>(),
            String::class.java,
        )
        val payloadCategory = jdbc.queryForObject(
            "SELECT payload ->> 'category' FROM stream_event",
            emptyMap<String, Any>(),
            String::class.java,
        )

        assertEquals(StreamCategory.REPORT.payload, category)
        assertEquals(StreamCategory.REPORT.eventType, eventType)
        assertEquals(StreamCategory.REPORT.payload, payloadCategory)
    }

    @Test
    fun `N4 DoD - digest 멱등 - 잡 2회 적재에도 브리핑 1건`() {
        xadd(newsEntry("hankyung:c1", "삼성전자 대규모 수주 낭보"))
        xadd(newsEntry("hankyung:c2", "삼성전자 공장 화재로 생산 차질 하락"))
        drain()

        val zone = ZoneId.of("Asia/Seoul")
        val now = ZonedDateTime.now(zone)
        val date = (if (now.hour >= 18) now.plusDays(1) else now).toLocalDate().toString()
        val digestEntry = IngestQueueEntry(
            source = IngestQueueEntry.DIGEST_SOURCE,
            sourceId = IngestQueueEntry.digestSourceId("005930", date),
            type = IngestType.DIGEST,
            codes = listOf("005930"),
            title = "",
            url = "",
            fetchedAt = System.currentTimeMillis(),
        )
        xadd(digestEntry)
        xadd(digestEntry)
        drain()

        val aiEvents = jdbc.queryForObject(
            "SELECT count(*) FROM stream_event WHERE code = '005930' AND type = 'AI'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        assertEquals(1, aiEvents)
        val positives = jdbc.queryForObject(
            "SELECT jsonb_array_length(payload -> 'digest' -> 'positives') FROM stream_event WHERE type = 'AI'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        val negatives = jdbc.queryForObject(
            "SELECT jsonb_array_length(payload -> 'digest' -> 'negatives') FROM stream_event WHERE type = 'AI'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        assertEquals(1, positives)
        assertEquals(1, negatives)
    }

    @Test
    fun `N5 DoD - poison 엔트리는 DLQ로 격리되고 PEL에서 사라진다`() {
        val poison = IngestConsumer(
            redis = redisTemplate,
            news = newsProcessor,
            digest = digestProcessor,
            marketDigest = marketDigestProcessor,
            meters = SimpleMeterRegistry(),
            props = LlmProperties(
                consumerBlock = Duration.ofMillis(200),
                poisonMaxDeliveries = 1,
                claimIdle = Duration.ZERO,
            ),
            consumerName = "poison-test",
        )
        val badDigest = mapOf(
            IngestQueueEntry.FIELD_SOURCE to IngestQueueEntry.DIGEST_SOURCE,
            IngestQueueEntry.FIELD_SOURCE_ID to "digest:broken:2026-07-16",
            IngestQueueEntry.FIELD_TYPE to IngestType.DIGEST.value,
            IngestQueueEntry.FIELD_CODES to "005930,000660",
            IngestQueueEntry.FIELD_TITLE to "",
            IngestQueueEntry.FIELD_URL to "",
            IngestQueueEntry.FIELD_FETCHED_AT to System.currentTimeMillis().toString(),
        )
        xadd(badDigest)
        poison.pollOnce()
        repeat(5) {
            if (dlqSize() > 0) return@repeat
            poison.claimStale()
        }

        assertEquals(1, dlqSize())
        val pending = redisTemplate.opsForStream<String, String>()
            .pending(Queues.INGEST, Queues.INGEST_GROUP_LLM)!!
        assertEquals(0, pending.totalPendingMessages)
    }

    @Test
    fun `N6 DoD - 매크로 기사 - 섹터 구성 종목 방으로 SECTOR 이벤트 fan-out`() {
        val received = subscribe(Channels.stream("105560"), Channels.stream("055550"), Channels.stream("086790"))
        try {
            xadd(newsEntry("hankyung:m1", "기준금리 인상에 은행 이자이익 개선 기대", codes = emptyList(), macroHint = "금리"))
            drain()

            assertEquals(listOf("055550", "086790", "105560"), sectorEventCodes())
            assertEquals(0, newsEventCount("005930"))
            val payload = jdbc.queryForObject(
                "SELECT payload::text FROM stream_event WHERE code = '105560'",
                emptyMap<String, Any>(),
                String::class.java,
            )!!
            assertTrue(payload.contains("\"SECTOR\""))
            assertTrue(payload.contains("은행"))
            assertEquals(
                setOf(Channels.stream("055550"), Channels.stream("086790"), Channels.stream("105560")),
                received.awaitChannels(3),
            )
        } finally {
            received.close()
        }
    }

    @Test
    fun `N6 - MEDIUM 이상만으로도 하드 상한을 넘으면 실시간 발행을 억제한다`() {
        val meters = SimpleMeterRegistry()
        val processor = newsProcessorWithFanoutCap(cap = 2, hardCap = 2, meters = meters)

        processor.process(newsEntry("hankyung:cap1", "기준금리 인상에 은행 이자이익 개선 기대", codes = emptyList()))

        assertTrue(sectorEventCodes().isEmpty())
        assertEquals(1.0, meters.counter("sector.fanout.suppressed").count())
        assertEquals(
            "SECTOR",
            jdbc.queryForObject("SELECT scope FROM news_cluster", emptyMap<String, Any>(), String::class.java),
        )
        assertEquals(
            1,
            jdbc.queryForObject("SELECT count(*) FROM news_cluster_sector", emptyMap<String, Any>(), Long::class.java),
        )
    }

    private fun sectorEventCodes(): List<String> = jdbc.queryForList(
        "SELECT code FROM stream_event WHERE type = 'NEWS' AND payload ->> 'scope' = 'SECTOR' ORDER BY code",
        emptyMap<String, Any>(),
        String::class.java,
    ).map(String::trim)

    private fun subscribe(vararg channels: String): Subscription {
        val queue = LinkedBlockingQueue<String>()
        val container = RedisMessageListenerContainer().apply {
            setConnectionFactory(redisTemplate.connectionFactory!!)
            channels.forEach { channel ->
                addMessageListener({ message, _ -> queue.add(String(message.channel)) }, ChannelTopic(channel))
            }
            afterPropertiesSet()
            start()
        }
        return Subscription(queue, container)
    }

    private class Subscription(
        private val queue: LinkedBlockingQueue<String>,
        private val container: RedisMessageListenerContainer,
    ) {
        fun awaitChannels(count: Int): Set<String> =
            (0 until count).mapNotNull { queue.poll(5, TimeUnit.SECONDS) }.toSet()

        fun close() = container.destroy()
    }

    @Test
    fun `후보 없이 수집된 기사는 STOCK 판정 후에도 같은 빈 후보 기사와 재클러스터링`() {
        xadd(newsEntry("hankyung:empty1", "코스피 정책 발표", codes = emptyList()))
        drain()

        val clusterId = jdbc.queryForObject(
            "SELECT id FROM news_cluster",
            emptyMap<String, Any>(),
            String::class.java,
        )!!
        jdbc.update(
            "UPDATE news_cluster SET scope = 'STOCK' WHERE id = :id",
            mapOf("id" to clusterId),
        )
        clusterStore.applyStockVerdicts(clusterId, listOf(StockVerdictWrite("005930", "NEUTRAL", 0.9, rejected = false)))

        xadd(newsEntry("maeil:empty2", "[속보] 코스피 정책 발표", source = "maeil", codes = emptyList()))
        drain()

        val clusterCount = jdbc.queryForObject(
            "SELECT count(*) FROM news_cluster",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        val articleCount = jdbc.queryForObject(
            "SELECT count(*) FROM news_article WHERE cluster_id = :id",
            mapOf("id" to clusterId),
            Long::class.java,
        )
        assertEquals(1, clusterCount)
        assertEquals(2, articleCount)
    }

    private fun dlqSize(): Long = redisTemplate.opsForStream<String, String>().size(Queues.INGEST_DLQ) ?: 0

    @Test
    fun `원문 요청 게이트는 호스트별 최소 간격을 공유한다`() {
        val uri = URI("https://news.example.com/article")
        val key = Keys.articleFetchRate("news.example.com")
        articleRequestGate.await(uri)
        val remaining = redisTemplate.getExpire(key, TimeUnit.MILLISECONDS)
        assertTrue(remaining > 0)

        val started = System.nanoTime()
        articleRequestGate.await(uri)
        val elapsedMillis = Duration.ofNanos(System.nanoTime() - started).toMillis()

        assertTrue(elapsedMillis >= (remaining - 20).coerceAtLeast(1))
    }

    @Test
    fun `요약 저장 트랜잭션이 실패하면 부분 링크를 롤백한다`() {
        val clusterId = "rollback-cluster".padEnd(26, '0')
        clusterStore.createCluster(clusterId, "롤백 검증", Instant.now())

        assertFailsWith<IllegalStateException> {
            transactions.run {
                clusterStore.addCandidateCode(clusterId, "005930")
                throw IllegalStateException("rollback")
            }
        }

        assertTrue(clusterStore.stockLinks(clusterId).isEmpty())
    }

    @Test
    fun `V3 - 같은 날짜 다이제스트 이중 삽입은 유니크 인덱스가 차단`() {
        val data = StreamData(
            category = "ai",
            title = "브리핑",
            occurredAt = 1,
            digest = DigestData(date = "2026-07-16"),
        )
        val first = eventStore.insertEvents(
            listOf(StreamEventRow("01ARZ3NDEKTSV4RRFFQ69G5FA1", "005930", "AI", Instant.now(), "worker-llm", data)),
        )
        val second = eventStore.insertEvents(
            listOf(StreamEventRow("01ARZ3NDEKTSV4RRFFQ69G5FA2", "005930", "AI", Instant.now(), "worker-llm", data)),
        )
        assertEquals(setOf("01ARZ3NDEKTSV4RRFFQ69G5FA1"), first)
        assertEquals(emptySet(), second)
    }

    @Test
    fun `배치 삽입은 일부만 충돌해도 실제로 삽입된 행만 발행 대상으로 돌려준다`() {
        val data = StreamData(category = "news", title = "뉴스", occurredAt = 1)
        val existing = "01ARZ3NDEKTSV4RRFFQ69G5FB0"
        eventStore.insertEvents(listOf(StreamEventRow(existing, "005930", "NEWS", Instant.now(), "worker-llm", data)))

        val inserted = eventStore.insertEvents(
            listOf(
                StreamEventRow(existing, "005930", "NEWS", Instant.now(), "worker-llm", data),
                StreamEventRow("01ARZ3NDEKTSV4RRFFQ69G5FB1", "000660", "NEWS", Instant.now(), "worker-llm", data),
                StreamEventRow(existing, "005930", "NEWS", Instant.now(), "worker-llm", data),
                StreamEventRow("01ARZ3NDEKTSV4RRFFQ69G5FB2", "005380", "NEWS", Instant.now(), "worker-llm", data),
            ),
        )

        assertEquals(setOf("01ARZ3NDEKTSV4RRFFQ69G5FB1", "01ARZ3NDEKTSV4RRFFQ69G5FB2"), inserted)
    }

    @Test
    fun `배치 이벤트 클레임은 이미 선점된 종목만 빼고 돌려준다`() {
        val clusterId = "01ARZ3NDEKTSV4RRFFQ69G5FC0"
        clusterStore.createCluster(clusterId, "배치 클레임", Instant.now())
        clusterStore.applyStockVerdicts(
            clusterId,
            listOf(
                StockVerdictWrite("005930", "POSITIVE", 0.9, rejected = false),
                StockVerdictWrite("000660", "POSITIVE", 0.9, rejected = false),
                StockVerdictWrite("005380", "POSITIVE", 0.9, rejected = false),
            ),
        )
        clusterStore.claimStockEvents(clusterId, mapOf("000660" to "01ARZ3NDEKTSV4RRFFQ69G5FC1"))

        val claimed = clusterStore.claimStockEvents(
            clusterId,
            mapOf(
                "005930" to "01ARZ3NDEKTSV4RRFFQ69G5FC2",
                "000660" to "01ARZ3NDEKTSV4RRFFQ69G5FC3",
                "005380" to "01ARZ3NDEKTSV4RRFFQ69G5FC4",
            ),
        )

        assertEquals(setOf("005930", "005380"), claimed)
        assertEquals(
            "01ARZ3NDEKTSV4RRFFQ69G5FC1",
            clusterStore.stockLinks(clusterId).single { it.code == "000660" }.streamEventId,
        )
    }

    @Test
    fun `N7 - market_digest 교체 규칙 - 완성본은 degraded 재실행으로 덮이지 않는다`() {
        val store = JdbcMarketDigestStore(jdbc, objectMapper)
        val degraded = MarketAnalysis(summary = "낮춰 생성", asOf = "2026-07-16T17:40:00+09:00", degraded = true)
        val complete = MarketAnalysis(summary = "완성본", asOf = "2026-07-16T18:10:00+09:00", degraded = false)

        assertTrue(store.save("2026-07-16", degraded))
        assertTrue(store.save("2026-07-16", complete))
        assertEquals(false, store.save("2026-07-16", degraded))
        assertEquals(false, store.save("2026-07-16", complete.copy(summary = "또 다른 완성본")))
        assertEquals("완성본", store.find("2026-07-16")!!.summary)
    }

    @Test
    fun `N7 DoD - MARKET 잡 소비 - market_digest 생성과 무입력 ACK`() {
        consumer.ensureGroup()
        val date = "2026-07-14"
        xadd(
            IngestQueueEntry(
                source = IngestQueueEntry.DIGEST_SOURCE,
                sourceId = IngestQueueEntry.digestSourceId(IngestQueueEntry.MARKET_CODE, date),
                type = IngestType.DIGEST,
                codes = listOf(IngestQueueEntry.MARKET_CODE),
                title = "",
                url = "",
                fetchedAt = System.currentTimeMillis(),
            ),
        )
        drain()

        assertEquals(0, consumer.samplePending())
        val rows = jdbc.queryForObject(
            "SELECT count(*) FROM market_digest WHERE date = CAST(:date AS date)",
            mapOf("date" to date),
            Long::class.java,
        )
        assertEquals(0L, rows)
    }

    @Test
    fun `N4+N6 - SECTOR fan-out 뉴스는 다이제스트 sectorIssues로 분류`() {
        xadd(newsEntry("hankyung:sd1", "기준금리 인상에 은행 이자이익 개선 기대", codes = emptyList(), macroHint = "금리"))
        drain()
        assertEquals(1, newsEventCount("105560"))

        val zone = ZoneId.of("Asia/Seoul")
        val now = ZonedDateTime.now(zone)
        val date = (if (now.hour >= 18) now.plusDays(1) else now).toLocalDate().toString()
        xadd(
            IngestQueueEntry(
                source = IngestQueueEntry.DIGEST_SOURCE,
                sourceId = IngestQueueEntry.digestSourceId("105560", date),
                type = IngestType.DIGEST,
                codes = listOf("105560"),
                title = "",
                url = "",
                fetchedAt = System.currentTimeMillis(),
            ),
        )
        drain()

        val positives = jdbc.queryForObject(
            "SELECT jsonb_array_length(payload -> 'digest' -> 'positives') FROM stream_event WHERE code = '105560' AND type = 'AI'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        val sectorIssues = jdbc.queryForObject(
            "SELECT jsonb_array_length(payload -> 'digest' -> 'sectorIssues') FROM stream_event WHERE code = '105560' AND type = 'AI'",
            emptyMap<String, Any>(),
            Long::class.java,
        )
        assertEquals(0, positives)
        assertEquals(1, sectorIssues)
    }

    @Nested
    @SpringBootTest(
        properties = [
            "alphatalk.llm.consume-enabled=false",
            "spring.datasource.hikari.data-source-properties.reWriteBatchedInserts=true",
        ],
    )
    inner class BatchCountUnavailable {
        @Autowired
        private lateinit var rewritingEventStore: StreamEventStore

        @Autowired
        private lateinit var rewritingJdbc: NamedParameterJdbcTemplate

        @Test
        fun `행 수를 알 수 없으면 삽입을 롤백하고 실패시킨다`() {
            val data = StreamData(category = "news", title = "뉴스", occurredAt = 1)
            val eventIds = listOf("01ARZ3NDEKTSV4RRFFQ69G5FE1", "01ARZ3NDEKTSV4RRFFQ69G5FE2")

            val failure = assertFailsWith<DataAccessException> {
                rewritingEventStore.insertEvents(
                    eventIds.map { StreamEventRow(it, "005930", "NEWS", Instant.now(), "worker-llm", data) },
                )
            }
            assertTrue(failure.message.orEmpty().contains("SUCCESS_NO_INFO"))

            val remaining = rewritingJdbc.queryForObject(
                "SELECT count(*) FROM stream_event WHERE event_id IN (:eventIds)",
                mapOf("eventIds" to eventIds),
                Long::class.java,
            )
            assertEquals(0L, remaining)
        }
    }
}
