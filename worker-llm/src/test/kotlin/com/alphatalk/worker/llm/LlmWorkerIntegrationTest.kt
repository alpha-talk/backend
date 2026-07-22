package com.alphatalk.worker.llm

import com.alphatalk.contracts.Channels
import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.envelope.DigestData
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.article.ArticleRequestGate
import com.alphatalk.worker.llm.cluster.ClusterStore
import com.alphatalk.worker.llm.consume.IngestConsumer
import com.alphatalk.worker.llm.enrich.DigestProcessor
import com.alphatalk.worker.llm.enrich.NewsProcessor
import com.alphatalk.worker.llm.enrich.TransactionRunner
import com.alphatalk.worker.llm.persist.StreamEventStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.data.redis.connection.stream.StreamRecords
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
        "alphatalk.llm.sector.coverage-stocks=105560,055550,086790",
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
    private lateinit var newsProcessor: NewsProcessor

    @Autowired
    private lateinit var eventStore: StreamEventStore

    @Autowired
    private lateinit var articleRequestGate: ArticleRequestGate

    @Autowired
    private lateinit var clusterStore: ClusterStore

    @Autowired
    private lateinit var transactions: TransactionRunner

    @BeforeEach
    fun reset() {
        redisTemplate.execute { it.serverCommands().flushAll() }
        jdbc.jdbcTemplate.execute(
            "TRUNCATE news_article, news_cluster_stock, news_cluster_sector, news_cluster, stream_event, stock_alias, stock_master, sector CASCADE",
        )
        jdbc.jdbcTemplate.execute("INSERT INTO sector (code, name) VALUES ('33', '반도체'), ('27', '은행')")
        jdbc.jdbcTemplate.execute(
            """
            INSERT INTO stock_master (code, name, sector_code) VALUES
            ('005930', '삼성전자', '33'), ('000660', 'SK하이닉스', '33'), ('105560', 'KB금융', '27')
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
    ) = IngestQueueEntry(
        source = source,
        sourceId = sourceId,
        type = IngestType.NEWS,
        codes = codes,
        title = title,
        url = "",
        body = null,
        fetchedAt = System.currentTimeMillis(),
        macroHint = macroHint,
    )

    private fun xadd(entry: IngestQueueEntry) {
        redisTemplate.opsForStream<String, String>().add(
            StreamRecords.mapBacked<String, String, String>(entry.toFields()).withStreamKey(Queues.INGEST),
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
            listener.stop()
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
            meters = SimpleMeterRegistry(),
            block = Duration.ofMillis(200),
            batch = 8,
            poisonMaxDeliveries = 1,
            claimIdle = Duration.ZERO,
            consumerName = "poison-test",
        )
        val badDigest = IngestQueueEntry(
            source = IngestQueueEntry.DIGEST_SOURCE,
            sourceId = "digest:broken:2026-07-16",
            type = IngestType.DIGEST,
            codes = listOf("005930", "000660"),
            title = "",
            url = "",
            fetchedAt = System.currentTimeMillis(),
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
        xadd(newsEntry("hankyung:m1", "기준금리 인상에 은행 이자이익 개선 기대", codes = emptyList(), macroHint = "금리"))
        drain()

        assertEquals(1, newsEventCount("105560"))
        assertEquals(0, newsEventCount("005930"))
        val payload = jdbc.queryForObject(
            "SELECT payload::text FROM stream_event WHERE code = '105560'",
            emptyMap<String, Any>(),
            String::class.java,
        )!!
        assertTrue(payload.contains("\"SECTOR\""))
        assertTrue(payload.contains("은행"))
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
        val first = eventStore.insertEvent("01ARZ3NDEKTSV4RRFFQ69G5FA1", "005930", "AI", Instant.now(), "worker-llm", data)
        val second = eventStore.insertEvent("01ARZ3NDEKTSV4RRFFQ69G5FA2", "005930", "AI", Instant.now(), "worker-llm", data)
        assertTrue(first)
        assertEquals(false, second)
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
}
