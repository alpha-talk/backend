package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.contracts.envelope.StreamData
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.article.ArticleFetcher
import com.alphatalk.worker.llm.cluster.ClusterAssigner
import com.alphatalk.worker.llm.cluster.ClusterContendedException
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.FakeEmbeddingClient
import com.alphatalk.worker.llm.cluster.InMemoryClusterStore
import com.alphatalk.worker.llm.cluster.NoopClusterLock
import com.alphatalk.worker.llm.config.LlmProperties
import com.alphatalk.worker.llm.persist.StreamEventStore
import com.alphatalk.worker.llm.publish.StreamPublisher
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.alphatalk.worker.llm.sector.SectorInfo
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NewsProcessorTest {
    private val store = InMemoryClusterStore()
    private val events = RecordingEventStore()
    private val publisher = RecordingPublisher()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T09:00:00Z"), ZoneOffset.UTC)
    private var verdict: ClusterSummaryOutput = stockVerdict()
    private val ids = AtomicInteger()

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

    private fun defaultLlm() = object : LlmClient {
        override fun summarize(input: ClusterSummaryInput) = verdict
        override fun digest(input: DigestInput) = DigestOutput("t", "s")
    }

    private fun processor(
        fanoutCap: Int = 100,
        fanoutHardCap: Int = 500,
        llm: LlmClient = defaultLlm(),
        meters: MeterRegistry = SimpleMeterRegistry(),
    ) = NewsProcessor(
        store = store,
        assigner = ClusterAssigner(
            store, FakeEmbeddingClient(64), NoopClusterLock(),
            LlmProperties(cluster = LlmProperties.Cluster(windowHours = 72, similarityThreshold = 0.85)),
            { "cl-${ids.incrementAndGet()}".padEnd(26, '0') }, clock,
        ),
        fetcher = ArticleFetcher { null },
        summarizer = ClusterSummarizer(llm),
        sectors = directory,
        events = events,
        publisher = publisher,
        eventIds = { "ev-${ids.incrementAndGet()}".padEnd(26, '0') },
        mapper = jacksonObjectMapper(),
        meters = meters,
        transactions = TransactionRunner { it() },
        props = LlmProperties(
            sector = LlmProperties.Sector(fanoutCap = fanoutCap, fanoutHardCap = fanoutHardCap),
        ),
        clock = clock,
    )

    private fun entry(
        sourceId: String,
        title: String,
        codes: List<String> = listOf("005930"),
        macroHint: String? = null,
        type: IngestType = IngestType.NEWS,
    ) = IngestQueueEntry(
        source = "hankyung",
        sourceId = sourceId,
        type = type,
        codes = codes,
        title = title,
        url = "https://example.com/$sourceId",
        fetchedAt = clock.millis(),
        macroHint = macroHint,
    )

    private fun stockVerdict() = ClusterSummaryOutput(
        summary = "3줄 요약",
        marketRelevant = true,
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
    fun `REPORT 수집 type은 REPORT 카테고리·이벤트로 관통 발행`() {
        processor().process(entry("r1", "삼성전자 목표가 상향", type = IngestType.REPORT))
        val event = events.inserted.single()
        assertEquals("REPORT", event.type)
        assertEquals("report", event.data.category)
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
    fun `후보 없는 기사 - LLM이 발견한 종목으로 발행`() {
        processor().process(entry("g1", "반도체 지원법 국회 통과", codes = emptyList()))
        assertEquals(listOf("005930"), events.inserted.map { it.code })
        assertEquals(listOf("005930"), publisher.published.map { it.first })
    }

    @Test
    fun `후보 없이 STOCK이 된 클러스터에 후속 빈 후보 기사가 재합류`() {
        val p = processor()

        p.process(entry("g1", "반도체 지원법 국회 통과", codes = emptyList()))
        p.process(entry("g2", "[속보] 반도체 지원법 국회 통과", codes = emptyList()))

        assertEquals(1, store.clusters.size)
        assertEquals(2, store.articles.size)
        assertEquals(1, events.inserted.size)
        assertEquals(1, publisher.published.size)
        assertTrue(events.refreshed.isNotEmpty())
    }

    @Test
    fun `LLM 발견 종목이 stock_master에 없으면 버린다`() {
        verdict = ClusterSummaryOutput(
            summary = "요약",
            marketRelevant = true,
            scope = NewsScope.STOCK,
            stocks = listOf(StockVerdict("999999", true, Sentiment.POSITIVE, 0.9, "환각 코드")),
            sectors = emptyList(),
        )
        processor().process(entry("h1", "출처 불명 뉴스", codes = emptyList()))
        assertTrue(events.inserted.isEmpty())
        assertTrue(publisher.published.isEmpty())
    }

    @Test
    fun `LLM 기각 종목은 재전달과 후속 기사 편입에서 다시 발행하지 않는다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 수주",
            marketRelevant = true,
            scope = NewsScope.STOCK,
            stocks = listOf(
                StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "직접 관련"),
                StockVerdict("000660", false, Sentiment.NEUTRAL, 0.8, "무관"),
            ),
            sectors = emptyList(),
        )
        val p = processor()
        val first = entry("a1", "삼성전자 수주", codes = listOf("005930", "000660"))

        p.process(first)
        p.process(first)
        p.process(entry("b1", "[속보] 삼성전자 수주", codes = listOf("005930", "000660")))

        assertEquals(listOf("005930"), events.inserted.map { it.code })
        assertEquals(true, store.stockLinks(store.clusters.keys.single()).single { it.code == "000660" }.rejected)
    }

    @Test
    fun `SECTOR - 구성 종목 fan-out에 섹터·감성 표기`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            marketRelevant = true,
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
    fun `SECTOR - 직접 관련 종목도 scope·sector 표기해 발행`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "직접")),
            sectors = listOf(SectorVerdict("33", Sentiment.POSITIVE, Impact.HIGH, 0.9, "")),
        )
        processor().process(entry("a1", "반도체 업황"))
        val direct = events.inserted.single { it.code == "005930" }
        assertEquals("SECTOR", direct.data.scope)
        assertEquals("반도체", direct.data.sector?.name)
    }

    @Test
    fun `SECTOR - impact LOW도 상한 이내면 실시간 배달한다`() {
        verdict = ClusterSummaryOutput(
            summary = "소폭 영향",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.9, "")),
        )
        processor().process(entry("a1", "업계 소식", codes = emptyList(), macroHint = "금리"))
        assertEquals(listOf("105560", "055550", "086790"), events.inserted.map { it.code })
        assertTrue(store.sectorLinkRows.containsKey(store.clusters.keys.single() to "27"))
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - 상한을 넘으면 LOW를 덜어내고 다시 배달한다`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(
                SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.9, "은행 3종목"),
                SectorVerdict("33", Sentiment.POSITIVE, Impact.HIGH, 0.9, "반도체 2종목"),
            ),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 4, meters = meters)
            .process(entry("a1", "기준금리 인상", codes = emptyList(), macroHint = "금리"))
        assertEquals(listOf("005930", "000660"), events.inserted.map { it.code })
        assertEquals(1.0, meters.counter("sector.fanout.degraded").count())
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - 같은 섹터를 impact 다르게 두 번 판정하면 LOW가 먼저 와도 높은 쪽을 쓴다`() {
        assertHighestSectorVerdictWins(
            listOf(
                SectorVerdict("33", Sentiment.NEUTRAL, Impact.LOW, 0.5, "중복 판정"),
                SectorVerdict("33", Sentiment.POSITIVE, Impact.HIGH, 0.9, ""),
            ),
        )
    }

    @Test
    fun `SECTOR - 같은 섹터 중복 판정은 HIGH가 먼저 와도 저장 행까지 높은 쪽으로 남는다`() {
        assertHighestSectorVerdictWins(
            listOf(
                SectorVerdict("33", Sentiment.POSITIVE, Impact.HIGH, 0.9, ""),
                SectorVerdict("33", Sentiment.NEUTRAL, Impact.LOW, 0.5, "중복 판정"),
            ),
        )
    }

    private fun assertHighestSectorVerdictWins(sectors: List<SectorVerdict>) {
        verdict = ClusterSummaryOutput(
            summary = "반도체 영향",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = sectors,
        )
        processor(fanoutCap = 1).process(entry("a1", "반도체 이슈", codes = emptyList()))

        assertEquals(listOf("005930", "000660"), events.inserted.map { it.code })
        assertTrue(events.inserted.all { it.data.sentiment == "POSITIVE" })
        assertEquals(
            Triple("POSITIVE", 0.9, "HIGH"),
            store.sectorLinkRows.getValue(store.clusters.keys.single() to "33"),
        )
    }

    @Test
    fun `SECTOR - 하드 상한 억제는 섹터 fan-out만 막고 소스 후보 종목은 발행한다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "소스 후보")),
            sectors = listOf(SectorVerdict("27", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, fanoutHardCap = 2, meters = meters)
            .process(entry("a1", "반도체 이슈", codes = listOf("005930")))

        assertEquals(listOf("005930"), events.inserted.map { it.code })
        assertEquals("POSITIVE", events.inserted.single().data.sentiment)
        assertEquals(1.0, meters.counter("sector.fanout.suppressed").count())
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - LLM이 후보 밖에서 발견한 종목은 상한 예외가 아니다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "LLM 기억으로 추가")),
            sectors = listOf(SectorVerdict("27", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, fanoutHardCap = 2, meters = meters)
            .process(entry("a1", "반도체 이슈", codes = emptyList()))

        assertTrue(events.inserted.isEmpty())
        assertEquals(1.0, meters.counter("sector.fanout.suppressed").count())
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `SECTOR - 섹터 밖 발견 종목은 상한 계산에 더해진다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("105560", true, Sentiment.POSITIVE, 0.9, "섹터 밖 발견")),
            sectors = listOf(SectorVerdict("33", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "2종목")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, meters = meters).process(entry("a1", "반도체 이슈", codes = emptyList()))

        assertEquals(1.0, meters.counter("sector.fanout.tier2").count())
        assertEquals(listOf("105560", "005930", "000660"), events.inserted.map { it.code })
    }

    @Test
    fun `SECTOR - LOW 강등 후에도 하드 상한 이내의 발견 종목은 발행한다`() {
        verdict = ClusterSummaryOutput(
            summary = "은행 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "섹터 밖 발견")),
            sectors = listOf(SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.5, "3종목")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 3, meters = meters).process(entry("a1", "은행 이슈", codes = emptyList()))

        assertEquals(listOf("005930"), events.inserted.map { it.code })
        assertEquals(1.0, meters.counter("sector.fanout.degraded").count())
        assertEquals(0.0, meters.counter("sector.fanout.suppressed").count())
    }

    @Test
    fun `SECTOR - material 구성원이 전부 예외 종목이면 억제가 아니라 강등이다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(
                StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "소스 후보"),
                StockVerdict("000660", true, Sentiment.POSITIVE, 0.9, "소스 후보"),
            ),
            sectors = listOf(
                SectorVerdict("33", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "구성원이 전부 소스 후보"),
                SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.5, "3종목"),
            ),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, meters = meters)
            .process(entry("a1", "반도체 이슈", codes = listOf("005930", "000660")))

        assertEquals(1.0, meters.counter("sector.fanout.degraded").count())
        assertEquals(0.0, meters.counter("sector.fanout.suppressed").count())
        assertEquals(listOf("005930", "000660"), events.inserted.map { it.code })
    }

    @Test
    fun `SECTOR - 기각된 소스 후보가 섹터 구성원이면 상한에 포함한다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", false, Sentiment.NEUTRAL, 0.5, "기각")),
            sectors = listOf(SectorVerdict("33", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "2종목")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 1, meters = meters)
            .process(entry("a1", "반도체 이슈", codes = listOf("005930")))

        assertEquals(1.0, meters.counter("sector.fanout.tier2").count())
    }

    @Test
    fun `SECTOR - 억제된 발견 종목도 클러스터 링크는 남겨 후속 기사가 합류한다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("105560", true, Sentiment.POSITIVE, 0.9, "LLM 발견")),
            sectors = listOf(SectorVerdict("27", Sentiment.NEGATIVE, Impact.HIGH, 0.9, "3종목")),
        )
        val p = processor(fanoutCap = 1, fanoutHardCap = 1)
        p.process(entry("a1", "반도체 이슈", codes = emptyList()))

        assertTrue(events.inserted.isEmpty())
        assertEquals(1, store.clusters.size)

        p.process(entry("a2", "[속보] 반도체 이슈", codes = listOf("105560")))

        assertEquals(1, store.clusters.size, "링크가 없어 새 클러스터가 생겼다")
        val event = events.inserted.single()
        assertEquals("105560", event.code)
        assertEquals("SECTOR", event.data.scope)
        assertEquals("27", event.data.sector?.code)
        assertEquals("POSITIVE", event.data.sentiment)
        val link = store.stockLinks(store.clusters.keys.single()).single { it.code == "105560" }
        assertEquals("POSITIVE", link.sentiment)
        assertEquals(0.9, link.confidence)
    }

    @Test
    fun `SECTOR - 발견 종목이 섹터 구성원이면 상한에 한 번만 센다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "섹터 구성원과 겹침")),
            sectors = listOf(SectorVerdict("33", Sentiment.NEGATIVE, Impact.LOW, 0.9, "2종목")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, meters = meters).process(entry("a1", "반도체 이슈", codes = emptyList()))

        assertEquals(0.0, meters.counter("sector.fanout.tier2").count())
        assertEquals(listOf("005930", "000660"), events.inserted.map { it.code })
    }

    @Test
    fun `SECTOR - 상한 예외인 소스 후보는 섹터 구성원이어도 상한 계산에서 빠진다`() {
        verdict = ClusterSummaryOutput(
            summary = "반도체 이슈",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = listOf(StockVerdict("005930", true, Sentiment.POSITIVE, 0.9, "소스 후보")),
            sectors = listOf(SectorVerdict("33", Sentiment.NEGATIVE, Impact.LOW, 0.9, "2종목")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 1, meters = meters)
            .process(entry("a1", "반도체 이슈", codes = listOf("005930")))

        assertEquals(0.0, meters.counter("sector.fanout.tier2").count())
        assertEquals(listOf("005930", "000660"), events.inserted.map { it.code })
    }

    @Test
    fun `SECTOR - LOW뿐인데 상한을 넘으면 강등이 아니라 억제로 집계한다`() {
        verdict = ClusterSummaryOutput(
            summary = "업계 소식",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.NEUTRAL, Impact.LOW, 0.9, "")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, meters = meters).process(entry("a1", "업계 소식", codes = emptyList()))

        assertEquals(0, events.inserted.size)
        assertEquals(1.0, meters.counter("sector.fanout.suppressed").count())
        assertEquals(0.0, meters.counter("sector.fanout.degraded").count())
    }

    @Test
    fun `SECTOR - LOW를 덜어낸 게 없으면 degraded로 집계하지 않는다`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.9, "")),
        )
        val meters = SimpleMeterRegistry()
        processor(fanoutCap = 2, meters = meters).process(entry("a1", "기준금리 인상", codes = emptyList()))

        assertEquals(3, events.inserted.size)
        assertEquals(0.0, meters.counter("sector.fanout.degraded").count())
        assertEquals(0.0, meters.counter("sector.fanout.suppressed").count())
    }

    @Test
    fun `SECTOR - MEDIUM 이상만으로도 하드 상한을 넘으면 실시간 발행을 억제한다`() {
        verdict = ClusterSummaryOutput(
            summary = "금리 인상",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = listOf(SectorVerdict("27", Sentiment.POSITIVE, Impact.HIGH, 0.9, "")),
        )
        processor(fanoutCap = 2, fanoutHardCap = 2)
            .process(entry("a1", "기준금리 인상", codes = emptyList(), macroHint = "금리"))
        assertEquals(0, events.inserted.size)
        assertTrue(store.sectorLinkRows.containsKey(store.clusters.keys.single() to "27"))
        assertEquals("SECTOR", store.clusters.values.single().scope)
    }

    @Test
    fun `MARKET - 방 fan-out 없음`() {
        verdict = ClusterSummaryOutput(
            summary = "코스피 급락",
            marketRelevant = true,
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
    fun `시장 무관 판정은 MARKET scope 응답이어도 IRRELEVANT`() {
        verdict = ClusterSummaryOutput(
            summary = "지역 축제 개막",
            marketRelevant = false,
            scope = NewsScope.MARKET,
            stocks = emptyList(),
            sectors = emptyList(),
        )

        processor().process(entry("social1", "지역 축제 개막", codes = emptyList()))

        assertEquals(ClusterStatus.IRRELEVANT, store.clusters.values.single().status)
        assertEquals(null, store.clusters.values.single().scope)
        assertTrue(events.inserted.isEmpty())
        assertTrue(publisher.published.isEmpty())
    }

    @Test
    fun `SECTOR - 관련 종목과 섹터가 모두 없으면 IRRELEVANT`() {
        verdict = ClusterSummaryOutput(
            summary = "무관",
            marketRelevant = true,
            scope = NewsScope.SECTOR,
            stocks = emptyList(),
            sectors = emptyList(),
        )
        processor().process(entry("a1", "업계 소식", codes = emptyList(), macroHint = "업계"))
        assertEquals(ClusterStatus.IRRELEVANT, store.clusters.values.single().status)
        assertTrue(events.inserted.isEmpty())
    }

    @Test
    fun `동시 요약 경쟁 - 한 워커만 요약하고 다른 워커는 contended`() {
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val llmCalls = AtomicInteger()
        val blockingLlm = object : LlmClient {
            override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
                llmCalls.incrementAndGet()
                entered.countDown()
                release.await(5, TimeUnit.SECONDS)
                return stockVerdict()
            }
            override fun digest(input: DigestInput) = DigestOutput("t", "s")
        }
        val p = processor(llm = blockingLlm)

        val worker1 = thread { p.process(entry("a1", "삼성전자 수주")) }
        assertTrue(entered.await(5, TimeUnit.SECONDS))

        var contended = false
        val worker2 = thread {
            try {
                p.process(entry("b1", "[속보] 삼성전자 수주"))
            } catch (e: ClusterContendedException) {
                contended = true
            }
        }
        worker2.join(5_000)

        assertTrue(contended)
        assertEquals(1, llmCalls.get())
        release.countDown()
        worker1.join(5_000)

        assertEquals(1, store.clusters.size)
        assertEquals(1, events.inserted.size)
        assertEquals(1, publisher.published.size)
    }

    @Test
    fun `만료 임대를 재선점하면 이전 소유자는 최종 저장할 수 없다`() {
        val id = "lease-cluster".padEnd(26, '0')
        val claimedAt = clock.instant()
        store.createCluster(id, "삼성전자 수주", claimedAt)

        assertTrue(store.claimSummarize(id, "old", claimedAt, claimedAt.minusSeconds(120)))
        assertTrue(store.claimSummarize(id, "new", claimedAt.plusSeconds(180), claimedAt.plusSeconds(60)))
        assertFalse(store.markSummarized(id, "old", "오래된 결과", "STOCK"))
        assertTrue(store.markSummarized(id, "new", "최신 결과", "STOCK"))
        assertEquals("최신 결과", store.clusters.getValue(id).summary)
    }

    @Test
    fun `전부 기각 - IRRELEVANT 마킹 후 발행 없음`() {
        verdict = ClusterSummaryOutput(
            summary = "무관",
            marketRelevant = true,
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

        @Synchronized
        override fun insertEvent(eventId: String, code: String, type: String, occurredAt: Instant, source: String?, data: StreamData): Boolean {
            if (inserted.any { it.eventId == eventId }) return false
            inserted.add(Inserted(eventId, code, type, data))
            return true
        }

        @Synchronized
        override fun refreshSources(eventId: String, sourcesJson: String) {
            refreshed.add(eventId)
        }

        override fun digestExists(code: String, date: String) =
            inserted.any { it.code == code && it.type == "AI" && it.data.digest?.date == date }
    }

    class RecordingPublisher : StreamPublisher {
        val published = mutableListOf<Pair<String, StreamData>>()

        @Synchronized
        override fun publish(code: String, eventId: String, data: StreamData) {
            published.add(code to data)
        }
    }
}
