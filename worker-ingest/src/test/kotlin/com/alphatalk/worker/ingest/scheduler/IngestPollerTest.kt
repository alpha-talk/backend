package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.dedup.SeenMarker
import com.alphatalk.worker.ingest.mapping.DictionaryStockCodeMapper
import com.alphatalk.worker.ingest.queue.IngestQueue
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class IngestPollerTest {
    private val mapper = DictionaryStockCodeMapper(
        stocks = mapOf("005930" to listOf("삼성전자")),
        macroKeywords = listOf("금리"),
    )
    private val seen = InMemorySeenMarker()
    private val queue = RecordingQueue()

    private fun poller(vararg sources: NewsSource) =
        IngestPoller(sources.toList(), mapper, seen, queue, excerptMaxLength = 200, clock = { 1719500000000 })

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
    fun `미매칭 기사는 적재도 seen 기록도 하지 않는다`() {
        val poller = poller(FakeSource("hankyung", listOf(FetchedArticle(title = "오늘의 날씨", url = "https://example.com/w"))))
        val stats = poller.pollOnce()
        assertEquals(1, stats.unmatchedSkipped)
        assertTrue(queue.entries.isEmpty())
        assertTrue(seen.marked.isEmpty())
    }

    @Test
    fun `매크로 기사 - codes 공란 + macroHint 적재`() {
        val poller = poller(FakeSource("hankyung", listOf(FetchedArticle(title = "한은 기준금리 인상", url = "https://example.com/m"))))
        poller.pollOnce()
        val entry = queue.entries.single()
        assertEquals(emptyList(), entry.codes)
        assertEquals("금리", entry.macroHint)
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
    fun `발췌는 상한 길이로 자른다`() {
        val poller = poller(
            FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자", url = "https://example.com/1", excerpt = "가".repeat(500)))),
        )
        poller.pollOnce()
        assertEquals(200, queue.entries.single().body?.length)
    }

    private class FakeSource(override val name: String, private val articles: List<FetchedArticle>) : NewsSource {
        override fun fetchLatest() = articles
    }

    private class FailingSource(override val name: String) : NewsSource {
        override fun fetchLatest(): List<FetchedArticle> = throw IllegalStateException("boom")
    }

    private class InMemorySeenMarker : SeenMarker {
        val marked = mutableSetOf<String>()
        override fun markIfNew(sourceId: String) = marked.add(sourceId)
        override fun clear(sourceId: String) {
            marked.remove(sourceId)
        }
    }

    private class RecordingQueue : IngestQueue {
        val entries = mutableListOf<IngestQueueEntry>()
        var failNext = false
        override fun enqueue(entry: IngestQueueEntry) {
            if (failNext) {
                failNext = false
                throw IllegalStateException("redis down")
            }
            entries.add(entry)
        }
    }
}
