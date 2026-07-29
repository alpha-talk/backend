package com.alphatalk.worker.ingest

import com.alphatalk.contracts.Queues
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.dedup.RedisSeenMarker
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
import kotlin.test.assertEquals
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

    @Test
    fun `seen 마커 - SETNX와 TTL`() {
        val marker = RedisSeenMarker(template, props)
        assertTrue(marker.markIfNew("hankyung:a1"))
        assertEquals(false, marker.markIfNew("hankyung:a1"))
        val ttl = template.getExpire("seen:ingest:hankyung:a1")
        assertTrue(ttl > 0)
    }

    @Test
    fun `큐 엔트리 필드 왕복 - XADD 후 재구성`() {
        val queue = RedisIngestQueue(template, props)
        val entry = IngestQueueEntry(
            source = "hankyung",
            sourceId = "hankyung:a1",
            type = com.alphatalk.contracts.queue.IngestType.NEWS,
            codes = listOf("005930"),
            title = "삼성전자 수주",
            url = "https://example.com/1",
            body = "발췌",
            fetchedAt = 1719500000000,
        )
        queue.enqueue(entry)

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
            seen = RedisSeenMarker(template, props),
            queue = RedisIngestQueue(template, props),
            props = props,
            fetchExecutor = { it.run() },
        )

        poller.pollOnce()
        poller.pollOnce()

        val size = template.opsForStream<String, String>().size(Queues.INGEST)
        assertEquals(1L, size)
    }
}
