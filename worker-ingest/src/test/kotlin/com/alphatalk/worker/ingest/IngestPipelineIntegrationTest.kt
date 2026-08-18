package com.alphatalk.worker.ingest

import com.alphatalk.contracts.Keys
import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.queue.EnqueueResult
import com.alphatalk.worker.ingest.queue.RedisIngestQueue
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.domain.Range
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertTrue

@Testcontainers
class IngestPipelineIntegrationTest {
    companion object {
        @Container
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        private val factory by lazy {
            LettuceConnectionFactory(redis.host, redis.getMappedPort(6379)).apply { afterPropertiesSet() }
        }
        private val template by lazy { StringRedisTemplate(factory) }

        @AfterAll
        @JvmStatic
        fun tearDown() {
            factory.destroy()
        }
    }

    private val props = IngestProperties(
        seenTtl = Duration.ofDays(7),
        queueMaxLen = 100,
    )

    @BeforeEach
    fun flush() {
        template.connectionFactory!!.connection.serverCommands().flushAll()
    }

    private fun newsEntry(sourceId: String = "hankyung:a1") = IngestQueueEntry(
        source = "hankyung",
        sourceId = sourceId,
        type = IngestType.NEWS,
        codes = listOf("005930"),
        title = "삼성전자 수주",
        url = "https://example.com/1",
        body = "발췌",
        fetchedAt = 1719500000000,
    )

    private fun digestEntry() = IngestQueueEntry(
        source = IngestQueueEntry.DIGEST_SOURCE,
        sourceId = IngestQueueEntry.digestSourceId("005930", "2026-08-18"),
        type = IngestType.DIGEST,
        codes = listOf("005930"),
        title = "",
        url = "",
        fetchedAt = 1775466000000,
    )

    @Test
    fun `기사 적재 - 마커가 TTL과 함께 남고 재적재는 중복으로 떨어진다`() {
        val queue = RedisIngestQueue(template, props)

        assertEquals(EnqueueResult.ENQUEUED, queue.enqueueIfNew(newsEntry()))
        assertEquals(EnqueueResult.ALREADY_ENQUEUED, queue.enqueueIfNew(newsEntry()))

        assertEquals(1L, template.opsForStream<String, String>().size(Queues.INGEST))
        val ttl = template.getExpire("seen:ingest:hankyung:a1")
        assertTrue(ttl > 0)
    }

    @Test
    fun `큐 엔트리 필드 왕복 - XADD 후 재구성`() {
        val queue = RedisIngestQueue(template, props)
        val entry = newsEntry()
        queue.enqueueIfNew(entry)

        val records = template.opsForStream<String, String>().range(Queues.INGEST, Range.unbounded())!!
        assertEquals(1, records.size)
        assertEquals(entry, IngestQueueEntry.fromFields(records.single().value))
    }

    @Test
    fun `재수집 멱등 - 같은 기사 두 번 폴링에 큐 적재 1건`() {
        val article = FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1")
        val source = object : NewsSource {
            override val name = "hankyung"
            override fun fetchLatest() = listOf(article)
        }
        val poller = IngestPoller(
            sources = listOf(source),
            queue = RedisIngestQueue(template, props),
            props = props,
            fetchExecutor = { it.run() },
        )

        poller.pollOnce()
        poller.pollOnce()

        val size = template.opsForStream<String, String>().size(Queues.INGEST)
        assertEquals(1L, size)
    }

    @Test
    fun `기사 원자 적재 - 동시 요청에도 큐와 마커는 한 건`() {
        val queue = RedisIngestQueue(template, props)
        val entry = newsEntry()

        val results = Executors.newFixedThreadPool(8).use { executor ->
            (1..32)
                .map { CompletableFuture.supplyAsync({ queue.enqueueIfNew(entry) }, executor) }
                .map { it.join() }
        }

        assertEquals(1, results.count { it == EnqueueResult.ENQUEUED })
        assertEquals(31, results.count { it == EnqueueResult.ALREADY_ENQUEUED })
        assertEquals(1L, template.opsForStream<String, String>().size(Queues.INGEST))
        assertTrue(template.hasKey(Keys.seenIngest(entry.sourceId)))
    }

    @Test
    fun `기사 원자 적재 - XADD 실패에는 마커를 남기지 않는다`() {
        val queue = RedisIngestQueue(template, props)
        val entry = newsEntry()
        template.opsForValue().set(Queues.INGEST, "wrong-type")

        assertFails { queue.enqueueIfNew(entry) }

        assertEquals(false, template.hasKey(Keys.seenIngest(entry.sourceId)))
        template.delete(Queues.INGEST)
        assertEquals(EnqueueResult.ENQUEUED, queue.enqueueIfNew(entry))
    }

    @Test
    fun `다이제스트 잡도 같은 원자 적재를 탄다`() {
        val queue = RedisIngestQueue(template, props)

        assertEquals(EnqueueResult.ENQUEUED, queue.enqueueIfNew(digestEntry()))
        assertEquals(EnqueueResult.ALREADY_ENQUEUED, queue.enqueueIfNew(digestEntry()))

        assertEquals(1L, template.opsForStream<String, String>().size(Queues.INGEST))
        assertTrue(template.hasKey(Keys.seenIngest(digestEntry().sourceId)))
    }
}
