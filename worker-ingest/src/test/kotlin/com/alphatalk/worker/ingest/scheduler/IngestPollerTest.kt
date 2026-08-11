package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class IngestPollerTest {
    private val sameThread = Executor { it.run() }
    private val seen = InMemorySeenMarker()
    private val queue = RecordingQueue()

    private fun poller(vararg sources: NewsSource) =
        IngestPoller(
            sources.toList(),
            seen,
            queue,
            props = IngestProperties(excerptMaxLength = 200),
            fetchExecutor = sameThread,
            clock = { 1719500000000 },
        )

    @Test
    fun `같은 기사를 두 번 폴링해도 큐 적재는 한 번`() {
        val article = FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1")
        val poller = poller(FakeSource("hankyung", listOf(article)))

        val first = poller.pollOnce()
        val second = poller.pollOnce()

        assertEquals(1, first.enqueued)
        assertEquals(1, second.duplicateSkipped)
        assertEquals(0, second.enqueued)
        assertEquals(1, queue.entries.size)
    }

    @Test
    fun `같은 언론사의 서로 다른 섹션에 실린 기사는 한 번만 적재`() {
        val article = FetchedArticle(
            sourceId = "news-0001",
            title = "삼성전자 수주",
            url = "https://example.com/1",
        )
        val poller = poller(
            FakeSource("yna", listOf(article), "yna-economy"),
            FakeSource("yna", listOf(article), "yna-society"),
        )

        val stats = poller.pollOnce()

        assertEquals(1, stats.enqueued)
        assertEquals(1, stats.duplicateSkipped)
        assertEquals("yna", queue.entries.single().source)
        assertEquals("yna:news-0001", queue.entries.single().sourceId)
    }

    @Test
    fun `추적 파라미터만 다른 같은 URL도 중복으로 잡는다`() {
        val poller = poller(
            FakeSource(
                "hankyung",
                listOf(
                    FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1?utm_source=a"),
                    FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1?utm_source=b"),
                ),
            ),
        )
        val stats = poller.pollOnce()
        assertEquals(1, stats.enqueued)
        assertEquals(1, stats.duplicateSkipped)
    }

    @Test
    fun `후보 없는 기사도 전량 적재 - 관련성 판정은 LLM 몫`() {
        val poller = poller(FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자 수주", url = "https://example.com/w"))))
        val stats = poller.pollOnce()
        assertEquals(1, stats.enqueued)
        val entry = queue.entries.single()
        assertEquals(emptyList(), entry.codes)
        assertEquals(null, entry.macroHint)
    }

    @Test
    fun `소스가 부여한 종목 후보는 정렬·중복 제거해 그대로 적재`() {
        val article = FetchedArticle(
            title = "반도체 투톱 동반 강세",
            url = "https://example.com/n",
            codes = listOf("005930", "000660", "005930"),
        )
        val poller = poller(FakeSource("dart", listOf(article)))
        poller.pollOnce()
        val entry = queue.entries.single()
        assertEquals(listOf("000660", "005930"), entry.codes)
    }

    @Test
    fun `적재 실패 시 seen 마커 롤백 - 다음 폴링에서 재시도`() {
        val article = FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1")
        val poller = poller(FakeSource("hankyung", listOf(article)))

        queue.failNext = true
        val first = poller.pollOnce()
        val second = poller.pollOnce()

        assertEquals(1, first.enqueueErrors)
        assertEquals(1, second.enqueued)
        assertEquals(1, queue.entries.size)
    }

    @Test
    fun `한 소스 실패가 다른 소스 수집을 막지 않는다`() {
        val poller = poller(
            FailingSource("broken"),
            FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1"))),
        )
        val stats = poller.pollOnce()
        assertEquals(1, stats.sourceErrors)
        assertEquals(1, stats.enqueued)
    }

    @Test
    fun `소스 fetch는 스레드 풀에서 병렬 실행된다`() {
        val barrier = CyclicBarrier(2)
        val sources = listOf("a", "b").map { sourceName ->
            object : NewsSource {
                override val name = sourceName
                override fun fetchLatest(): List<FetchedArticle> {
                    barrier.await(2, TimeUnit.SECONDS)
                    return listOf(FetchedArticle(title = "삼성전자 $sourceName", url = "https://example.com/$sourceName"))
                }
            }
        }
        val pool = Executors.newFixedThreadPool(2)
        try {
            val poller = IngestPoller(
                sources, seen, queue,
                props = IngestProperties(excerptMaxLength = 200),
                fetchExecutor = pool,
            )
            val stats = poller.pollOnce()
            assertEquals(0, stats.sourceErrors)
            assertEquals(2, stats.fetched)
            assertEquals(2, stats.enqueued)
        } finally {
            pool.shutdown()
        }
    }

    @Test
    fun `병렬 실행에서도 소스별 통계가 합산된다`() {
        val pool = Executors.newFixedThreadPool(4)
        try {
            val poller = IngestPoller(
                listOf(
                    FakeSource("a", listOf(FetchedArticle(title = "삼성전자 A", url = "https://example.com/a"))),
                    FakeSource("b", listOf(FetchedArticle(title = "오늘의 날씨", url = "https://example.com/w"))),
                    FailingSource("c"),
                ),
                seen, queue, props = IngestProperties(excerptMaxLength = 200), fetchExecutor = pool,
            )
            val stats = poller.pollOnce()
            assertEquals(2, stats.enqueued)
            assertEquals(1, stats.sourceErrors)
        } finally {
            pool.shutdown()
        }
    }

    @Test
    fun `발췌는 상한 길이로 자른다`() {
        val poller = poller(
            FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자", url = "https://example.com/1", excerpt = "가".repeat(500)))),
        )
        poller.pollOnce()
        assertEquals(200, queue.entries.single().body?.length)
    }

    private class FakeSource(
        override val name: String,
        private val articles: List<FetchedArticle>,
        override val id: String = name,
    ) : NewsSource {
        override fun fetchLatest() = articles
    }

    private class FailingSource(override val name: String) : NewsSource {
        override fun fetchLatest(): List<FetchedArticle> = throw IllegalStateException("boom")
    }

    private class InMemorySeenMarker : SeenMarker {
        val marked = mutableSetOf<String>()

        @Synchronized
        override fun markIfNew(sourceId: String) = marked.add(sourceId)

        @Synchronized
        override fun clear(sourceId: String) {
            marked.remove(sourceId)
        }
    }

    private class RecordingQueue : IngestQueue {
        val entries = mutableListOf<IngestQueueEntry>()
        var failNext = false

        @Synchronized
        override fun enqueue(entry: IngestQueueEntry) {
            if (failNext) {
                failNext = false
                throw IllegalStateException("redis down")
            }
            entries.add(entry)
        }
    }
}
