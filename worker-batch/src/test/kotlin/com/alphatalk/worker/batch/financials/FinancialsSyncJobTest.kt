package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartApiException
import com.alphatalk.worker.batch.industry.DartClient
import com.alphatalk.worker.batch.industry.DartCompany
import com.alphatalk.worker.batch.industry.DartCorp
import com.alphatalk.worker.batch.industry.DartDisclosure
import com.alphatalk.worker.batch.industry.DartFinancialAccount
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.stockinfo.ActiveStock
import com.alphatalk.worker.batch.stockinfo.StockUniverse
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class FinancialsSyncJobTest {
    private val meters = SimpleMeterRegistry()
    private val runs = FakeRuns()
    private val store = FakeFinancialStore()
    private val today = LocalDate.parse("2026-08-07")

    private val cfsAccounts = listOf(
        DartFinancialAccount("BS", "ifrs-full_Assets", "자산총계", null, 4000),
        DartFinancialAccount("IS", "ifrs-full_Revenue", "매출액", null, 3000),
    )

    private fun disclosure(
        code: String = "005930",
        corp: String = "00126380",
        name: String = "반기보고서 (2026.06)",
        receipt: String = "20260805",
    ) = DartDisclosure(corpCode = corp, stockCode = code, reportName = name, receiptDate = receipt)

    private fun job(
        dart: DartClient,
        active: Set<String> = setOf("005930", "000660"),
        lookbackDays: Long = 7,
    ) = FinancialsSyncJob(
        dart = dart,
        universe = universe(active),
        store = store,
        runs = runs,
        meters = meters,
        lookbackDays = lookbackDays,
        requestInterval = Duration.ZERO,
        today = { today },
        pause = {},
    )

    private fun universe(codes: Set<String>) = object : StockUniverse {
        override fun activeStocks(): List<ActiveStock> = codes.map { ActiveStock(it, null) }
        override fun activeCodes(): Set<String> = codes
    }

    private open class StubDart(
        private val disclosures: List<DartDisclosure>,
        private val accounts: Map<String, List<DartFinancialAccount>>,
    ) : DartClient {
        val requested = mutableListOf<String>()

        override fun corpCodes(): List<DartCorp> = emptyList()
        override fun company(corpCode: String): DartCompany? = null

        override fun periodicDisclosures(begin: LocalDate, end: LocalDate): List<DartDisclosure> {
            requested += "list:$begin..$end"
            return disclosures
        }

        override fun financialAccounts(
            corpCode: String,
            year: Int,
            reprtCode: String,
            fsDiv: String,
        ): List<DartFinancialAccount> {
            requested += "fnltt:$corpCode:$year:$reprtCode:$fsDiv"
            return accounts["$corpCode:$fsDiv"].orEmpty()
        }
    }

    @Test
    fun `감지한 정기공시를 연결 우선으로 적재한다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))

        val stored = job(dart).syncOnce()

        assertEquals(1, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        val row = store.rows.single()
        assertEquals("005930", row.code)
        assertEquals(2026, row.year)
        assertEquals("11012", row.reprtCode)
        assertEquals("CFS", row.fsDiv)
        assertEquals(3000, row.figures.revenue)
        assertEquals(Instant.parse("2026-08-04T15:00:00Z"), row.disclosedAt)
        assertTrue(dart.requested.contains("list:2026-07-31..2026-08-07"))
    }

    @Test
    fun `연결이 없으면 별도 재무제표로 폴백한다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:OFS" to cfsAccounts))

        job(dart).syncOnce()

        assertEquals("OFS", store.rows.single().fsDiv)
        assertTrue(dart.requested.contains("fnltt:00126380:2026:11012:CFS"))
        assertTrue(dart.requested.contains("fnltt:00126380:2026:11012:OFS"))
    }

    @Test
    fun `양쪽 다 없으면 적재 없이 성공하고 absent를 센다`() {
        val dart = StubDart(listOf(disclosure()), emptyMap())

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertEquals(0, runs.lastFailCount)
        assertEquals(1.0, meters.counter("batch.financials.absent").count())
    }

    @Test
    fun `비상장·비활성 종목과 비정기 공시는 걸러낸다`() {
        val dart = StubDart(
            listOf(
                disclosure(code = "999999"),
                DartDisclosure("00000001", null, "사업보고서 (2025.12)", "20260805"),
                disclosure(name = "주요사항보고서(유상증자결정)"),
            ),
            emptyMap(),
        )

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertTrue(dart.requested.none { it.startsWith("fnltt") })
    }

    @Test
    fun `같은 보고서의 중복 공시는 최신 접수일 하나로 합친다`() {
        val dart = StubDart(
            listOf(
                disclosure(receipt = "20260801"),
                disclosure(name = "[기재정정]반기보고서 (2026.06)", receipt = "20260806"),
            ),
            mapOf("00126380:CFS" to cfsAccounts),
        )

        val stored = job(dart).syncOnce()

        assertEquals(1, stored)
        assertEquals(Instant.parse("2026-08-05T15:00:00Z"), store.rows.single().disclosedAt)
        assertEquals(1, dart.requested.count { it == "fnltt:00126380:2026:11012:CFS" })
    }

    @Test
    fun `일시 오류는 말미에 1회 재시도하고 잔여 실패는 fail_count로 남긴다`() {
        var calls = 0
        val dart = object : StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts)) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                calls += 1
                if (calls == 1) throw DartApiException("unknown", "timeout")
                return super.financialAccounts(corpCode, year, reprtCode, fsDiv)
            }
        }

        val stored = job(dart).syncOnce()

        assertEquals(1, stored)
        assertEquals(0, runs.lastFailCount)
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `마스터가 오늘 실패로 남아 있으면 유예한다 - 부분 유니버스로 SUCCESS를 굳히지 않는다`() {
        runs.failedJobs += "stock_master_sync"
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("FAILED"), runs.finished)
        assertTrue(dart.requested.isEmpty())
    }

    @Test
    fun `마스터 행이 아직 없으면 진행한다 - 06시 정규 회차는 마스터보다 이르다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))

        val stored = job(dart).syncOnce()

        assertEquals(1, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `재실행 진입점도 이미 성공한 날은 조회하지 않는다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))
        val skipping = object : BatchJobRunStore {
            override fun start(job: String, runDate: String, startedAt: Instant): Long? = null
            override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L
            override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) = Unit
            override fun fail(id: Long, error: String, finishedAt: Instant) = Unit
        }
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = store,
            runs = skipping,
            meters = meters,
            requestInterval = Duration.ZERO,
            today = { today },
            pause = {},
        )

        job.scheduledRetry()

        assertTrue(dart.requested.isEmpty())
    }

    @Test
    fun `재시도까지 실패한 공시가 남으면 FAILED다 - 다음 날엔 감지 창을 벗어난다`() {
        val dart = object : StubDart(listOf(disclosure()), emptyMap()) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> = throw DartApiException("unknown", "timeout")
        }

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, runs.lastFailCount)
    }

    @Test
    fun `감지한 보고서 코드 그대로만 조회한다 - 다른 분기로 대체하지 않는다`() {
        val dart = StubDart(listOf(disclosure(name = "분기보고서 (2026.09)")), emptyMap())

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertTrue(dart.requested.none { it.startsWith("fnltt") && !it.contains(":11014:") })
        assertEquals(1.0, meters.counter("batch.financials.absent").count())
    }

    @Test
    fun `데드라인에 도달하면 FAILED로 남겨 수동 재실행을 연다`() {
        var now = Instant.parse("2026-08-06T21:00:00Z")
        val dart = object : StubDart(
            listOf(disclosure(), disclosure(code = "000660", corp = "00164742")),
            mapOf("00126380:CFS" to cfsAccounts, "00164742:CFS" to cfsAccounts),
        ) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                now = now.plusSeconds(101 * 60)
                return super.financialAccounts(corpCode, year, reprtCode, fsDiv)
            }
        }
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930", "000660")),
            store = store,
            runs = runs,
            meters = meters,
            requestInterval = Duration.ZERO,
            deadline = Duration.ofMinutes(100),
            clock = { now },
            today = { today },
            pause = {},
        )

        val stored = job.syncOnce()

        assertEquals(1, stored)
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1.0, meters.counter("batch.financials.deadline").count())
    }

    @Test
    fun `유니버스가 비면 FAILED다 - 선행 마스터 동기화 부재는 성공이 아니다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))

        assertFailsWith<IllegalStateException> { job(dart, active = emptySet()).syncOnce() }

        assertEquals(listOf("FAILED"), runs.finished)
        assertTrue(dart.requested.isEmpty())
    }

    @Test
    fun `저장 실패는 재시도 대상이 아니라 잡 실패다`() {
        val dart = StubDart(listOf(disclosure()), mapOf("00126380:CFS" to cfsAccounts))
        val brokenStore = object : FinancialSummaryStore {
            override fun upsert(row: FinancialSummaryRow) = throw IllegalStateException("db down")
        }
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = brokenStore,
            runs = runs,
            meters = meters,
            requestInterval = Duration.ZERO,
            today = { today },
            pause = {},
        )

        assertFailsWith<IllegalStateException> { job.syncOnce() }
        assertEquals(listOf("FAILED"), runs.finished)
    }

    @Test
    fun `인증·한도 오류는 잡을 실패시킨다`() {
        val dart = object : StubDart(listOf(disclosure()), emptyMap()) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> = throw DartApiException("020", "요청 제한 초과")
        }

        assertFailsWith<DartApiException> { job(dart).syncOnce() }
        assertEquals(listOf("FAILED"), runs.finished)
    }

    @Test
    fun `매핑 불가 계정만 있으면 적재하지 않고 unmapped를 센다`() {
        val dart = StubDart(
            listOf(disclosure()),
            mapOf("00126380:CFS" to listOf(DartFinancialAccount("IS", "dart_unknown", "지분법손익", null, 12))),
        )

        val stored = job(dart).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertEquals(1.0, meters.counter("batch.financials.unmapped").count())
    }

    private class FakeRuns : BatchJobRunStore {
        val finished = mutableListOf<String>()
        val failedJobs = mutableSetOf<String>()
        var lastFailCount = -1

        override fun start(job: String, runDate: String, startedAt: Instant): Long = 1L
        override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L

        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            finished += "SUCCESS"
            lastFailCount = failCount
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            finished += "FAILED"
        }

        override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
            finished += "FAILED"
            lastFailCount = failCount
        }

        override fun hasFailedRun(job: String, runDate: String): Boolean = job in failedJobs
    }

    private class FakeFinancialStore : FinancialSummaryStore {
        val rows = mutableListOf<FinancialSummaryRow>()

        override fun upsert(row: FinancialSummaryRow) {
            rows += row
        }
    }
}
