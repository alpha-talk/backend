package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.InMemoryClusterStore
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.alphatalk.worker.llm.sector.SectorInfo
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DigestProcessorTest {
    private val store = InMemoryClusterStore()
    private val events = NewsProcessorTest.RecordingEventStore()
    private val publisher = NewsProcessorTest.RecordingPublisher()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T09:30:00Z"), ZoneOffset.UTC)
    private var ids = 0

    private val directory = object : SectorDirectory {
        override fun allSectors() = listOf(SectorInfo("33", "반도체"))
        override fun sectorName(sectorCode: String) = "반도체"
        override fun memberCodes(sectorCode: String) = listOf("005930", "000660")
        override fun stockName(stockCode: String) = "삼성전자"
        override fun sectorOf(stockCode: String) = "33"
    }

    private val processor = DigestProcessor(
        store = store,
        sectors = directory,
        events = events,
        publisher = publisher,
        eventIds = { "dg-${++ids}".padEnd(26, '0') },
        llm = FakeLlmClient(),
        clock = clock,
    )

    private fun digestEntry(code: String = "005930", date: String = "2026-07-16") = IngestQueueEntry(
        source = IngestQueueEntry.DIGEST_SOURCE,
        sourceId = IngestQueueEntry.digestSourceId(code, date),
        type = IngestType.DIGEST,
        codes = listOf(code),
        title = "",
        url = "",
        fetchedAt = clock.millis(),
    )

    private fun seedCluster(id: String, title: String, sentiment: Sentiment?, at: Instant, scope: String = "STOCK", code: String? = "005930") {
        store.createCluster(id, title, at)
        store.clusters.getValue(id).let {
            it.status = ClusterStatus.SUMMARIZED
            it.summary = "$title 요약"
            it.scope = scope
        }
        code?.let {
            store.insertStockLinkIfAbsent(id, it, sentiment?.name, 0.9)
            store.setStockLinkEvent(id, it, "ev-$id".take(26).padEnd(26, '0'))
        }
    }

    private val inWindow: Instant = Instant.parse("2026-07-16T02:00:00Z")

    @Test
    fun `호재·악재 버킷 분리 + 섹터·시장 이슈 취합`() {
        seedCluster("c1".padEnd(26, '0'), "3나노 수주", Sentiment.POSITIVE, inWindow)
        seedCluster("c2".padEnd(26, '0'), "공장 화재", Sentiment.NEGATIVE, inWindow)
        seedCluster("c3".padEnd(26, '0'), "단순 소식", Sentiment.NEUTRAL, inWindow)
        seedCluster("c4".padEnd(26, '0'), "반도체 업황 개선", null, inWindow, scope = "SECTOR", code = null)
        store.upsertSectorLink("c4".padEnd(26, '0'), "33", "POSITIVE", 0.9, "HIGH")
        seedCluster("c5".padEnd(26, '0'), "외국인 순매도", null, inWindow, scope = "MARKET", code = null)

        processor.process(digestEntry())

        val ai = events.inserted.single()
        assertEquals("AI", ai.type)
        val digest = ai.data.digest!!
        assertEquals(listOf("3나노 수주"), digest.positives.map { it.title })
        assertEquals(listOf("공장 화재"), digest.negatives.map { it.title })
        assertEquals(listOf("반도체 업황 개선"), digest.sectorIssues.map { it.title })
        assertEquals(listOf("외국인 순매도"), digest.marketIssues.map { it.title })
        assertEquals(1, digest.neutralCount)
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `멱등 - 같은 날짜 두 번 처리해도 브리핑 1건`() {
        seedCluster("c1".padEnd(26, '0'), "3나노 수주", Sentiment.POSITIVE, inWindow)
        processor.process(digestEntry())
        processor.process(digestEntry())
        assertEquals(1, events.inserted.size)
    }

    @Test
    fun `시장 이슈만 있으면 브리핑을 만들지 않는다`() {
        seedCluster("c5".padEnd(26, '0'), "외국인 순매도", null, inWindow, scope = "MARKET", code = null)
        processor.process(digestEntry())
        assertTrue(events.inserted.isEmpty())
    }

    @Test
    fun `윈도 밖 클러스터는 제외`() {
        seedCluster("c1".padEnd(26, '0'), "옛날 뉴스", Sentiment.POSITIVE, Instant.parse("2026-07-10T02:00:00Z"))
        processor.process(digestEntry())
        assertTrue(events.inserted.isEmpty())
    }
}
