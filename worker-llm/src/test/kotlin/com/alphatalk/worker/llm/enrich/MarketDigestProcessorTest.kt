package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.MarketAnalysis
import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.cluster.ClusterStatus
import com.alphatalk.worker.llm.cluster.InMemoryClusterStore
import com.alphatalk.worker.llm.config.LlmProperties
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class MarketDigestProcessorTest {
    private val store = InMemoryClusterStore()
    private val digests = InMemoryMarketDigestStore()
    private val facts = FakeFactSheetSource()
    private val llm = FakeMarketLlm()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T10:00:00Z"), ZoneOffset.UTC)

    private fun processor(researchEnabled: Boolean = true) = MarketDigestProcessor(
        store = store,
        facts = facts,
        digests = digests,
        llm = llm,
        props = LlmProperties(market = LlmProperties.Market(researchEnabled = researchEnabled)),
        meters = SimpleMeterRegistry(),
        clock = clock,
    )

    private fun entry(date: String = "2026-07-16") = IngestQueueEntry(
        source = IngestQueueEntry.DIGEST_SOURCE,
        sourceId = IngestQueueEntry.digestSourceId(IngestQueueEntry.MARKET_CODE, date),
        type = IngestType.DIGEST,
        codes = listOf(IngestQueueEntry.MARKET_CODE),
        title = "",
        url = "",
        fetchedAt = clock.millis(),
    )

    private fun seedMarketCluster(id: String = "mkt-1") {
        store.createCluster(id, "코스피 급락", Instant.parse("2026-07-16T05:00:00Z"))
        store.clusters.getValue(id).let {
            it.status = ClusterStatus.SUMMARIZED
            it.summary = "요약"
            it.scope = "MARKET"
        }
    }

    private val sheet = MarketFactSheet(
        factDate = "2026-07-16",
        advancers = 900, decliners = 1500, unchanged = 200,
        topSectors = listOf(MarketFactSheet.SectorPerformance("2차전지", 3.2, 12)),
        bottomSectors = listOf(MarketFactSheet.SectorPerformance("반도체", -1.8, 40)),
        foreignNetBuyTop = listOf(MarketFactSheet.SectorFlow("2차전지", 1_000)),
        institutionNetBuyTop = listOf(MarketFactSheet.SectorFlow("은행", 500)),
    )

    @Test
    fun `세 층이 갖춰지면 완성본을 저장한다`() {
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.output = researchedOutput()

        processor().process(entry())

        val saved = digests.saved.getValue("2026-07-16")
        assertFalse(saved.degraded)
        assertEquals("2026-07-16", saved.factDate)
        assertEquals(listOf("s1"), saved.global.single().sourceIds)
        assertTrue(llm.lastInput!!.research)
        assertEquals(1, llm.lastInput!!.marketClusters.size)
    }

    @Test
    fun `팩트시트 커버리지 미달이면 factDate 없이 degraded로 저장한다`() {
        facts.result = FactSheetLookup.Insufficient
        seedMarketCluster()
        llm.output = researchedOutput()

        processor().process(entry())

        val saved = digests.saved.getValue("2026-07-16")
        assertTrue(saved.degraded)
        assertNull(saved.factDate)
    }

    @Test
    fun `리서치가 기대됐는데 global이 비면 degraded다`() {
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.output = MarketDigestOutput("요약", emptyList(), emptyList(), emptyList())

        processor().process(entry())

        assertTrue(digests.saved.getValue("2026-07-16").degraded)
    }

    @Test
    fun `검색 미지원 provider면 리서치 없이 degraded로 생성한다`() {
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.researchSupported = false
        llm.output = MarketDigestOutput("요약", emptyList(), emptyList(), emptyList())

        processor().process(entry())

        assertFalse(llm.lastInput!!.research)
        assertTrue(digests.saved.getValue("2026-07-16").degraded)
    }

    @Test
    fun `전 층을 성공 조회했는데 모두 비면 저장 없이 끝난다`() {
        facts.result = FactSheetLookup.Missing
        llm.researchSupported = false

        processor().process(entry())

        assertTrue(digests.saved.isEmpty())
        assertNull(llm.lastInput)
    }

    @Test
    fun `내용이 없는데 조회 실패가 있으면 잡 실패로 던진다`() {
        facts.failing = true
        llm.researchSupported = false

        assertThrows<IllegalStateException> { processor().process(entry()) }
        assertTrue(digests.saved.isEmpty())
    }

    @Test
    fun `리서치가 실패하고 남은 내용도 없으면 ACK가 아니라 잡 실패다`() {
        facts.result = FactSheetLookup.Missing
        llm.failWhenResearching = true
        llm.output = MarketDigestOutput("빈", emptyList(), emptyList(), emptyList())

        assertThrows<IllegalStateException> { processor().process(entry()) }
        assertTrue(digests.saved.isEmpty())
    }

    @Test
    fun `팩트시트 부분 적재만 있고 다른 내용이 없으면 잡 실패로 재시도한다`() {
        facts.result = FactSheetLookup.Insufficient
        llm.researchSupported = false

        assertThrows<IllegalStateException> { processor().process(entry()) }
        assertTrue(digests.saved.isEmpty())
    }

    @Test
    fun `리서치 호출이 실패하면 리서치 없이 재호출해 degraded로 생성한다`() {
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.failWhenResearching = true
        llm.output = MarketDigestOutput("국내만", emptyList(), emptyList(), emptyList())

        processor().process(entry())

        assertFalse(llm.lastInput!!.research)
        assertTrue(digests.saved.getValue("2026-07-16").degraded)
    }

    @Test
    fun `무리서치 재호출이 지어낸 global은 버려지고 완성본으로 굳지 않는다`() {
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.failWhenResearching = true
        llm.output = researchedOutput()

        processor().process(entry())

        val saved = digests.saved.getValue("2026-07-16")
        assertTrue(saved.degraded)
        assertTrue(saved.global.isEmpty())
        assertTrue(saved.sources.isEmpty())
    }

    @Test
    fun `완성본이 있으면 재처리해도 LLM을 부르지 않는다`() {
        digests.saved["2026-07-16"] = analysis(degraded = false)

        processor().process(entry())

        assertNull(llm.lastInput)
    }

    @Test
    fun `degraded 저장본은 재처리에서 완성본으로 상향된다`() {
        digests.saved["2026-07-16"] = analysis(degraded = true)
        facts.result = FactSheetLookup.Found(sheet)
        seedMarketCluster()
        llm.output = researchedOutput()

        processor().process(entry())

        assertFalse(digests.saved.getValue("2026-07-16").degraded)
    }

    private fun researchedOutput() = MarketDigestOutput(
        summary = "종합 3줄",
        domestic = listOf(MarketAnalysis.DomesticItem("순환매", "반도체→2차전지")),
        global = listOf(MarketAnalysis.GlobalItem("미 10년물 하락", "인하 기대", listOf("s1"))),
        sources = listOf(MarketAnalysis.ResearchSource("s1", "기사", "https://example.com", "Reuters")),
    )

    private fun analysis(degraded: Boolean) = MarketAnalysis(
        summary = "기존", asOf = "2026-07-16T17:40:00+09:00", degraded = degraded,
    )

    private class FakeFactSheetSource : MarketFactSheetSource {
        var result: FactSheetLookup = FactSheetLookup.Missing
        var failing = false
        override fun lookup(onOrBefore: LocalDate): FactSheetLookup {
            if (failing) throw IllegalStateException("db down")
            return result
        }
    }

    private class FakeMarketLlm : LlmClient {
        var researchSupported = true
        var failWhenResearching = false
        var output = MarketDigestOutput("m", emptyList(), emptyList(), emptyList())
        var lastInput: MarketDigestInput? = null

        override fun summarize(input: ClusterSummaryInput) = throw UnsupportedOperationException()
        override fun digest(input: DigestInput) = throw UnsupportedOperationException()
        override fun marketDigest(input: MarketDigestInput): MarketDigestOutput {
            if (input.research && failWhenResearching) throw IllegalStateException("search down")
            lastInput = input
            return output
        }

        override fun supportsMarketResearch() = researchSupported
    }
}
