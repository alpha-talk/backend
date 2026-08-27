package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.DigestData
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.InMemoryClusterStore
import com.alphatalk.worker.llm.config.LlmProperties
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

    private val marketDigests = InMemoryMarketDigestStore()

    private val processor = newProcessor()

    private fun newProcessor(
        llm: LlmClient = FakeLlmClient(),
        digest: LlmProperties.Digest = LlmProperties.Digest(),
    ) = DigestProcessor(
        store = store,
        sectors = directory,
        events = events,
        marketDigests = marketDigests,
        publisher = publisher,
        eventIds = { "dg-${++ids}".padEnd(26, '0') },
        llm = llm,
        props = LlmProperties(digest = digest),
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
            store.applyStockVerdict(id, it, sentiment?.name, 0.9, rejected = false)
            store.claimStockEvent(id, it, "ev-$id".take(26).padEnd(26, '0'))
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
    fun `시장 다이제스트가 있으면 브리핑에 marketAnalysis로 삽입한다`() {
        seedCluster("c1".padEnd(26, '0'), "3나노 수주", Sentiment.POSITIVE, inWindow)
        marketDigests.saved["2026-07-16"] = com.alphatalk.contracts.envelope.MarketAnalysis(
            summary = "시장 종합", asOf = "2026-07-16T17:40:00+09:00", factDate = "2026-07-16",
        )

        processor.process(digestEntry())

        assertEquals("시장 종합", events.inserted.single().data.digest!!.marketAnalysis!!.summary)
    }

    @Test
    fun `시장 다이제스트 조회가 실패해도 브리핑은 marketAnalysis 없이 생성된다`() {
        seedCluster("c1".padEnd(26, '0'), "3나노 수주", Sentiment.POSITIVE, inWindow)
        marketDigests.failing = true

        processor.process(digestEntry())

        val digest = events.inserted.single().data.digest!!
        assertEquals(null, digest.marketAnalysis)
    }

    @Test
    fun `멱등 - 같은 날짜 두 번 처리해도 브리핑 1건`() {
        seedCluster("c1".padEnd(26, '0'), "3나노 수주", Sentiment.POSITIVE, inWindow)
        processor.process(digestEntry())
        processor.process(digestEntry())
        assertEquals(1, events.inserted.size)
    }

    @Test
    fun `SECTOR fan-out 클러스터는 positives가 아니라 sectorIssues로 분류`() {
        val id = "c7".padEnd(26, '0')
        seedCluster(id, "반도체 업황 개선", Sentiment.POSITIVE, inWindow, scope = "SECTOR")
        store.upsertSectorLink(id, "33", "POSITIVE", 0.9, "HIGH")

        processor.process(digestEntry())

        val digest = events.inserted.single().data.digest!!
        assertTrue(digest.positives.isEmpty())
        assertEquals(listOf("반도체 업황 개선"), digest.sectorIssues.map { it.title })
        assertEquals(store.stockLinks(id).single().streamEventId, digest.sectorIssues.single().eventId)
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

    @Test
    fun `입력과 payload를 유형별 상한으로 제한하고 전체 건수는 보존한다`() {
        repeat(7) { index ->
            seedCluster("p$index".padEnd(26, '0'), "호재 $index", Sentiment.POSITIVE, inWindow.plusSeconds(index.toLong()))
            seedCluster("n$index".padEnd(26, '0'), "악재 $index", Sentiment.NEGATIVE, inWindow.plusSeconds(index.toLong()))
        }
        repeat(5) { index ->
            seedCluster("z$index".padEnd(26, '0'), "중립 $index", Sentiment.NEUTRAL, inWindow.plusSeconds(index.toLong()))
        }
        repeat(7) { index ->
            val id = "s$index".padEnd(26, '0')
            seedCluster(id, "섹터 $index", null, inWindow.plusSeconds(index.toLong()), scope = "SECTOR", code = null)
            store.upsertSectorLink(
                id,
                "33",
                Sentiment.POSITIVE.name,
                0.9,
                if (index == 6) Impact.HIGH.name else Impact.LOW.name,
            )
        }
        repeat(5) { index ->
            seedCluster(
                "m$index".padEnd(26, '0'),
                "시장 $index",
                null,
                inWindow.plusSeconds(index.toLong()),
                scope = "MARKET",
                code = null,
            )
        }
        lateinit var received: DigestInput
        val recordingLlm = object : LlmClient {
            override fun summarize(input: ClusterSummaryInput) = error("unused")
            override fun digest(input: DigestInput): DigestOutput {
                received = input
                return DigestOutput("브리핑", "요약")
            }
            override fun marketDigest(input: MarketDigestInput) = error("unused")
        }

        newProcessor(llm = recordingLlm).process(digestEntry())

        val digest = events.inserted.single().data.digest!!
        assertEquals(5, digest.positives.size)
        assertEquals(5, digest.negatives.size)
        assertEquals(5, digest.sectorIssues.size)
        assertEquals(3, digest.marketIssues.size)
        assertEquals(DigestData.Counts(stock = 19, sector = 7, market = 5), digest.inputCounts)
        assertEquals(DigestData.Counts(stock = 13, sector = 5, market = 3), digest.includedCounts)
        assertEquals(2, digest.pipelineVersion)
        assertEquals(5, digest.neutralCount)
        assertEquals(13, received.stockClusters.size)
        assertEquals("섹터 6", received.sectorClusters.first().title)
        assertEquals(listOf("시장 4", "시장 3", "시장 2"), received.marketClusters.map { it.title })
    }
}
