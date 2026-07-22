package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.FakeEmbeddingClient
import com.alphatalk.worker.llm.cluster.InMemoryClusterStore
import com.alphatalk.worker.llm.cluster.NoopClusterLock
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.alphatalk.worker.llm.sector.SectorInfo
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class NewsProcessorTest {
    private val store = InMemoryClusterStore()
    private val events = RecordingEventStore()
    private val publisher = RecordingPublisher()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T09:00:00Z"), ZoneOffset.UTC)
    private var verdict: ClusterSummaryOutput = stockVerdict()
    private var ids = 0

    private val directory = object : SectorDirectory {
        override fun allSectors() = listOf(SectorInfo("27", "은행"), SectorInfo("33", "반도체"))
        override fun sectorName(sectorCode: String) = allSectors().firstOrNull { it.code == sectorCode }?.name
        override fun memberCodes(sectorCode: String) = when (sectorCode) {
            "27" -> listOf("105560", "055550", "086790")
            "33" -> listOf("005930", "000660")
            else -> emptyList()
        }
        override fun stockName(stockCode: String) = mapOf(
            "005930" to "삼성전자", "000660" to "SK하이닉스",
            "105560" to "KB금융", "055550" to "신한지주", "086790" to "하나금융지주",
        )[stockCode]
        override fun sectorOf(stockCode: String) = if (stockCode in listOf("005930", "000660")) "33" else "27"
    }

    private fun processor(fanoutCap: Int = 100, coverage: List<String> = emptyList()) = NewsProcessor(
        store = store,
        assigner = ClusterAssigner(
            store, FakeEmbeddingClient(64), NoopClusterLock(),
            Duration.ofHours(72), 0.85, { "cl-${++ids}".padEnd(26, '0') }, clock,
        ),
        fetcher = ArticleFetcher { null },
        summarizer = ClusterSummarizer(
            object : LlmClient {
                override fun summarize(input: ClusterSummaryInput) = verdict
                override fun digest(input: DigestInput) = DigestOutput("t", "s")
            },
        ),
        sectors = directory,
        events = events,
        publisher = publisher,
        eventIds = { "ev-${++ids}".padEnd(26, '0') },
        mapper = jacksonObjectMapper(),
        meters = SimpleMeterRegistry(),
        fanoutCap = fanoutCap,
        coverageStocks = coverage,
        clock = clock,
    )

    private fun entry(sourceId: String, title: String, codes: List<String> = listOf("005930"), macroHint: String? = null) =
        IngestQueueEntry(
            source = "hankyung",
            sourceId = sourceId,
            type = IngestType.NEWS,
            codes = codes,
            title = title,
            url = "https://example.com/$sourceId",
            fetchedAt = clock.millis(),
            macroHint = macroHint,
        )

    private fun stockVerdict() = ClusterSummaryOutput(
        summary = "3줄 요약",
        scope = NewsScope.STOCK,
        stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "수주")),
        sectors = emptyList(),
    )

    @Test
    fun `STOCK - 관련 종목별 이벤트 저장·발행`() {
        processor().process(entry("a1", "삼성전자 수주"))
        assertEquals(1, events.inserted.size)
        assertEquals("NEWS", events.inserted.single().type)
        assertEquals("POSITIVE", events.inserted.single().data.sentiment)
        assertEquals(listOf("005930"), publisher.published.map { it.first })
    }

    @Test
    fun `멱등 - 처리 완료 기사 재처리는 아무것도 안 한다`() {
        val p = processor()
        p.process(entry("a1", "삼성전자 수주"))
        p.process(entry("a1", "삼성전자 수주"))
        assertEquals(1, events.inserted.size)
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `편입 - sources 갱신만 하고 재발행하지 않는다`() {
        val p = processor()
        p.process(entry("a1", "삼성전자 수주"))
        p.process(entry("b1", "[속보] 삼성전자 수주"))
        assertEquals(1, events.inserted.size)
        assertEquals(1, publisher.published.size)
        assertTrue(events.refreshed.isNotEmpty())
    }

    @Test
    fun `편입 기사에 새 종목 - 그 종목만 신규 발행`() {
        val p = processor()
        p.process(entry("a1", "삼성전자 수주"))
        p.process(entry("b1", "[속보] 삼성전자 수주", codes = listOf("005930", "000660")))
        assertEquals(2, events.inserted.size)
        assertEquals(listOf("005930", "000660"), publisher.published.map { it.first })
    }

    @Test
    fun `SECTOR - 구성 종목 fan-out에 섹터·감성 표기`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.9, "이자이익")),
        )
        processor().process(entry("a1", "기준금리 인상", codes = emptyList(), macroHint = "금리"))
        assertEquals(3, events.inserted.size)
        events.inserted.forEach {
            assertEquals("SECTOR", it.data.scope)
            assertEquals("은행", it.data.sector?.name)
            assertEquals("POSITIVE", it.data.sentiment)
        }
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - 커버리지 교집합만 배달`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.9, "")),
        )
        processor(coverage = listOf("105560")).process(entry("a1", "기준금리 인상", codes = emptyList(), macroHint = "금리"))
        assertEquals(listOf("105560"), events.inserted.map { it.code })
    }

    @Test
    fun `SECTOR - fan-out 상한 초과면 MARKET 강등`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.9, "")),
        )
        processor(fanoutCap = 2).process(entry("a1", "기준금리 인상", codes = emptyList(), macroHint = "금리"))
        assertEquals(0, events.inserted.size)
        assertEquals("MARKET", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - impact LOW는 실시간 fan-out 없이 링크만`() {
        verdict = ClusterSummaryOutput(
            summary = "소폭 영향",
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.9, "")),
        )
        processor().process(entry("a1", "업계 소식", codes = emptyList(), macroHint = "금리"))
        assertEquals(0, events.inserted.size)
        assertTrue(store.sectorLinkRows.containsKey(store.clusters.keys.single() to "27"))
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `MARKET - 방 fan-out 없음`() {
        verdict = ClusterSummaryOutput(
            summary = "코스피 급락",
            scope = NewsScope.MARKET,
            stocks = emptyList(),
            sectors = emptyList(),
        )
        processor().process(entry("a1", "코스피 급락", codes = emptyList(), macroHint = "증시"))
        assertEquals(0, events.inserted.size)
        assertEquals("MARKET", store.clusters.values.single().scope)
        assertEquals(ClusterStatus.SUMMARIZED, store.clusters.values.single().status)
    }

    @Test
    fun `전부 기각 - IRRELEVANT 마킹 후 발행 없음`() {
        verdict = ClusterSummaryOutput(
            summary = "무관",
            scope = NewsScope.STOCK,
            stocks = listOf(StockVerdict("005930", false, Sentiment.NEUTRAL, 0.9, "무관")),
            sectors = emptyList(),
        )
        processor().process(entry("a1", "삼성 라이온즈 우승"))
        assertEquals(0, events.inserted.size)
        assertEquals(ClusterStatus.IRRELEVANT, store.clusters.values.single().status)
    }

    class RecordingEventStore : StreamEventStore {
        data class Inserted(val eventId: String, val code: String, val type: String, val data: StreamData)

        val inserted = mutableListOf<Inserted>()
        val refreshed = mutableListOf<String>()

        override fun insertEvent(eventId: String, code: String, type: String, occurredAt: Instant, source: String?, data: StreamData): Boolean {
            if (inserted.any { it.eventId == eventId }) return false
            inserted.add(Inserted(eventId, code, type, data))
            return true
        }

        override fun refreshSources(eventId: String, sourcesJson: String) {
            refreshed.add(eventId)
        }

        override fun digestExists(code: String, date: String) =
            inserted.any { it.code == code && it.type == "AI" && it.data.digest?.date == date }
    }

    class RecordingPublisher : StreamPublisher {
        val published = mutableListOf<Pair<String, StreamData>>()
        override fun publish(code: String, eventId: String, data: StreamData) {
            published.add(code to data)
        }
    }
}
