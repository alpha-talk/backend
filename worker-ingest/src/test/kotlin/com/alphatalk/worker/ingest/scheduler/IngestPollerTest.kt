package com.alphatalk.worker.ingest.scheduler

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.inbox.FeedCheckpoint
import com.alphatalk.worker.ingest.inbox.IngestInbox
import com.alphatalk.worker.ingest.source.FeedUpdate
import com.alphatalk.worker.ingest.source.FeedValidators
import com.alphatalk.worker.ingest.source.FetchedArticle
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import org.springframework.dao.DataIntegrityViolationException
import java.sql.SQLException
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals

class IngestPollerTest {
    private val sameThread = Executor { it.run() }
    private val inbox = RecordingInbox()

    private fun poller(vararg sources: NewsSource) =
        IngestPoller(
            sources.toList(),
            inbox,
            props = IngestProperties(excerptMaxLength = 200),
            fetchExecutor = sameThread,
            clock = { 1719500000000 },
        )

    @Test
    fun `같은 기사를 두 번 폴링해도 수집본은 하나만 저장한다`() {
        val article = FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1")
        val poller = poller(FakeSource("hankyung", listOf(article)))

        val first = poller.pollOnce()
        val second = poller.pollOnce()

        assertEquals(1, first.captured)
        assertEquals(1, second.duplicateSkipped)
        assertEquals(0, second.captured)
        assertEquals(1, inbox.entries.size)
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

        assertEquals(1, stats.captured)
        assertEquals(1, stats.duplicateSkipped)
        assertEquals("yna", inbox.entries.single().source)
        assertEquals("yna:news-0001", inbox.entries.single().sourceId)
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
        assertEquals(1, stats.captured)
        assertEquals(1, stats.duplicateSkipped)
    }

    @Test
    fun `후보 없는 기사도 전량 적재 - 관련성 판정은 LLM 몫`() {
        val poller = poller(FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자 수주", url = "https://example.com/w"))))
        val stats = poller.pollOnce()
        assertEquals(1, stats.captured)
        val entry = inbox.entries.single()
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
        val entry = inbox.entries.single()
        assertEquals(listOf("000660", "005930"), entry.codes)
    }

    @Test
    fun `수집본 저장 실패 후 다음 폴링에서 재시도한다`() {
        val article = FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1")
        val poller = poller(FakeSource("hankyung", listOf(article)))

        inbox.nextFailure = IllegalStateException("capture failed")
        val first = poller.pollOnce()
        val second = poller.pollOnce()

        assertEquals(1, first.sourceErrors)
        assertEquals(1, second.captured)
        assertEquals(1, inbox.entries.size)
    }

    @Test
    fun `동시 중복 저장 충돌 후 같은 RSS 응답으로 남은 기사를 저장한다`() {
        val sharedArticle = FetchedArticle(sourceId = "shared", title = "공통 기사", url = "https://example.com/shared")
        val otherArticle = FetchedArticle(sourceId = "other", title = "새 기사", url = "https://example.com/other")
        val source = FakeSource("yna", listOf(sharedArticle, otherArticle), "yna-industry")
        poller(FakeSource("yna", listOf(sharedArticle), "yna-economy")).pollOnce()
        inbox.nextFailure = DataIntegrityViolationException("duplicate source ID", SQLException("duplicate key", "23505"))

        val stats = poller(source).pollOnce()

        assertEquals(1, source.fetchCalls)
        assertEquals(PollStats(fetched = 2, captured = 1, duplicateSkipped = 1), stats)
        assertEquals(listOf("yna:shared", "yna:other"), inbox.entries.map { it.sourceId })
    }

    @Test
    fun `한 소스 실패가 다른 소스 수집을 막지 않는다`() {
        val poller = poller(
            FailingSource("broken"),
            FakeSource("hankyung", listOf(FetchedArticle(title = "삼성전자 수주", url = "https://example.com/1"))),
        )
        val stats = poller.pollOnce()
        assertEquals(1, stats.sourceErrors)
        assertEquals(1, stats.captured)
    }

    @Test
    fun `소스 fetch는 스레드 풀에서 병렬 실행된다`() {
        val barrier = CyclicBarrier(2)
        val sources = listOf("a", "b").map { sourceName ->
            object : NewsSource {
                override val name = sourceName
                override val url = "https://example.com/rss"
                override fun fetchLatest(validators: FeedValidators): FeedUpdate {
                    barrier.await(2, TimeUnit.SECONDS)
                    return FeedUpdate(listOf(FetchedArticle(title = "삼성전자 $sourceName", url = "https://example.com/$sourceName")))
                }
            }
        }
        val pool = Executors.newFixedThreadPool(2)
        try {
            val poller = IngestPoller(
                sources, inbox,
                props = IngestProperties(excerptMaxLength = 200),
                fetchExecutor = pool,
            )
            val stats = poller.pollOnce()
            assertEquals(0, stats.sourceErrors)
            assertEquals(2, stats.fetched)
            assertEquals(2, stats.captured)
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
                inbox, props = IngestProperties(excerptMaxLength = 200), fetchExecutor = pool,
            )
            val stats = poller.pollOnce()
            assertEquals(2, stats.captured)
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
        assertEquals(200, inbox.entries.single().body?.length)
    }

    private class FakeSource(
        override val name: String,
        private val articles: List<FetchedArticle>,
        override val id: String = name,
    ) : NewsSource {
        override val url = "https://example.com/rss"
        var fetchCalls = 0
            private set

        override fun fetchLatest(validators: FeedValidators): FeedUpdate {
            fetchCalls++
            return FeedUpdate(articles)
        }
    }

    private class FailingSource(override val name: String) : NewsSource {
        override val url = "https://example.com/rss"
        override fun fetchLatest(validators: FeedValidators): FeedUpdate = throw IllegalStateException("boom")
    }

    private class RecordingInbox : IngestInbox {
        val entries = mutableListOf<IngestQueueEntry>()
        private val sourceIds = mutableSetOf<String>()
        var nextFailure: RuntimeException? = null

        override fun checkpoint(feedId: String, url: String): FeedCheckpoint? = null

        @Synchronized
        override fun capture(
            feedId: String,
            url: String,
            previous: FeedCheckpoint?,
            validators: FeedValidators,
            entries: List<IngestQueueEntry>,
        ): Int {
            nextFailure?.let {
                nextFailure = null
                throw it
            }
            val captured = entries.filter { sourceIds.add(it.sourceId) }
            this.entries.addAll(captured)
            return captured.size
        }
    }
}
