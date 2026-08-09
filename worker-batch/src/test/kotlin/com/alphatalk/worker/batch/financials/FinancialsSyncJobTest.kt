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
        backfillPerRun: Int = 0,
        failureStreakLimit: Int = 5,
        corps: Map<String, String> = emptyMap(),
    ) = FinancialsSyncJob(
        dart = dart,
        universe = universe(active),
        store = store,
        corps = FakeCorps(corps),
        runs = runs,
        meters = meters,
        lookbackDays = lookbackDays,
        backfillPerRun = backfillPerRun,
        failureStreakLimit = failureStreakLimit,
        requestInterval = Duration.ZERO,
        today = { today },
        pause = {},
    )

    private fun universe(codes: Set<String>) = object : StockUniverse {
        override fun activeStocks(): List<ActiveStock> = codes.map { ActiveStock(it, null) }
        override fun activeCodes(): Set<String> = codes
    }

    private class FakeCorps(private val mapping: Map<String, String>) : CorpDirectory {
        override fun corpCodesFor(codes: Collection<String>): Map<String, String> =
            mapping.filterKeys { it in codes }
    }

    private open class StubDart(
        private val disclosures: List<DartDisclosure>,
        private val accounts: Map<String, List<DartFinancialAccount>>,
        private val corpDisclosures: Map<String, List<DartDisclosure>> = emptyMap(),
    ) : DartClient {
        val requested = mutableListOf<String>()

        override fun corpCodes(): List<DartCorp> = emptyList()
        override fun company(corpCode: String): DartCompany? = null

        override fun periodicDisclosures(begin: LocalDate, end: LocalDate, corpCode: String?): List<DartDisclosure> {
            requested += "list:$begin..$end${corpCode?.let { ":$it" } ?: ""}"
            return if (corpCode == null) disclosures else corpDisclosures[corpCode].orEmpty()
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
    fun `연속 실패가 한도에 닿으면 회차를 중단한다 - 전면 장애가 전 종목 호출을 소모하지 않게`() {
        val disclosures = (1..8).map { n -> disclosure(code = "00593$n", corp = "0012638$n") }
        val dart = object : StubDart(disclosures, emptyMap()) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                super.financialAccounts(corpCode, year, reprtCode, fsDiv)
                throw DartApiException("unknown", "connection reset")
            }
        }

        job(dart, active = disclosures.mapTo(mutableSetOf()) { it.stockCode!! }, failureStreakLimit = 3).syncOnce()

        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1.0, meters.counter("batch.financials.breaker").count())
        assertEquals(3, dart.requested.count { it.startsWith("fnltt") && it.endsWith(":CFS") })
    }

    @Test
    fun `HTTP 429와 DART 900은 즉시 잡을 실패시킨다`() {
        listOf("429", "503", "900").forEach { status ->
            val localRuns = FakeRuns()
            val dart = object : StubDart(listOf(disclosure()), emptyMap()) {
                override fun financialAccounts(
                    corpCode: String,
                    year: Int,
                    reprtCode: String,
                    fsDiv: String,
                ): List<DartFinancialAccount> = throw DartApiException(status, "operational failure")
            }
            val job = FinancialsSyncJob(
                dart = dart,
                universe = universe(setOf("005930")),
                store = store,
                corps = FakeCorps(emptyMap()),
                runs = localRuns,
                meters = meters,
                requestInterval = Duration.ZERO,
                today = { today },
                pause = {},
            )

            assertFailsWith<DartApiException> { job.syncOnce() }
            assertEquals(listOf("FAILED"), localRuns.finished)
        }
    }

    @Test
    fun `행이 없는 종목을 회사 단위 3년 창으로 백필한다`() {
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(
                    disclosure(name = "사업보고서 (2025.12)", receipt = "20260310"),
                    disclosure(name = "분기보고서 (2026.03)", receipt = "20260515"),
                ),
            ),
        )

        val stored = job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertEquals(2, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        val corpLists = dart.requested.filter { it.startsWith("list:") && it.endsWith(":00126380") }
        assertEquals(
            listOf(
                "list:2023-08-07..2024-08-06:00126380",
                "list:2024-08-07..2025-08-06:00126380",
                "list:2025-08-07..2026-08-06:00126380",
                "list:2026-08-07..2026-08-07:00126380",
            ),
            corpLists,
        )
        assertEquals(setOf(2025 to "11011", 2026 to "11013"), store.rows.mapTo(mutableSetOf()) { it.year to it.reprtCode })
        assertEquals(1.0, meters.counter("batch.financials.backfill.stocks").count())
    }

    @Test
    fun `백필 완료 마커가 있는 종목과 corp 매핑이 없는 종목은 백필하지 않는다`() {
        store.backfilled["005930"] = BackfillMarker(3, FinancialAccountMapper.MAPPING_VERSION)
        val dart = StubDart(emptyList(), emptyMap())

        job(
            dart,
            active = setOf("005930", "000660"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertTrue(dart.requested.none { it.contains(":00126380") })
        assertEquals(1.0, meters.counter("batch.financials.backfill.unmapped").count())
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `증분이 먼저 적재한 과거 행은 완료가 아니다 - 마커 없는 종목은 백필된다`() {
        store.rows += historicalRow("005930", 2024)
        store.rows += historicalRow("005930", 2023)
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(disclosure(name = "사업보고서 (2023.12)", receipt = "20240310")),
            ),
        )

        val stored = job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertEquals(1, stored)
        assertTrue(store.rows.any { it.year == 2023 })
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    private fun historicalRow(code: String, year: Int) = FinancialSummaryRow(
        code = code,
        year = year,
        reprtCode = "11011",
        fsDiv = "CFS",
        figures = FinancialFigures(1, 1, 1, 1, 1, 1),
        disclosedAt = Instant.parse("2026-03-09T15:00:00Z"),
    )

    @Test
    fun `백필은 회차당 상한 개수만 처리하고 나머지는 다음 회차로 미룬다`() {
        val corps = (1..5).associate { n -> "00000$n" to "0000000$n" }
        val dart = StubDart(emptyList(), emptyMap())

        job(
            dart,
            active = corps.keys,
            backfillPerRun = 2,
            corps = corps,
        ).syncOnce()

        val corpsCalled = dart.requested.mapNotNull { entry ->
            entry.takeIf { it.startsWith("list:") }?.substringAfterLast(":")?.takeIf { it.startsWith("0000000") }
        }.toSet()
        assertEquals(2, corpsCalled.size)
    }

    @Test
    fun `백필 창은 날짜로 회전한다 - 영구 부적격 종목이 선두를 점유해도 나머지가 굶지 않는다`() {
        val corps = (1..5).associate { n -> "00000$n" to "0000000$n" }

        fun corpsCalledOn(date: LocalDate): Set<String> {
            val dart = StubDart(emptyList(), emptyMap())
            FinancialsSyncJob(
                dart = dart,
                universe = universe(corps.keys),
                store = FakeFinancialStore(),
                corps = FakeCorps(corps),
                runs = FakeRuns(),
                meters = meters,
                backfillPerRun = 2,
                requestInterval = Duration.ZERO,
                today = { date },
                pause = {},
            ).syncOnce()
            return dart.requested.mapNotNull { entry ->
                entry.takeIf { it.startsWith("list:") }?.substringAfterLast(":")?.takeIf { it.startsWith("0000000") }
            }.toSet()
        }

        assertEquals(corpsCalledOn(today), corpsCalledOn(today))
        val covered = (0L..4L).flatMapTo(mutableSetOf()) { corpsCalledOn(today.plusDays(it)) }
        assertEquals(corps.values.toSet(), covered)
    }

    @Test
    fun `증분이 오늘 공시를 먼저 적재해도 과거 커버리지가 없으면 백필 대상이다`() {
        val dart = StubDart(
            disclosures = listOf(disclosure()),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(disclosure(name = "사업보고서 (2024.12)", receipt = "20250310")),
            ),
        )

        val stored = job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertEquals(2, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertEquals(setOf(2026 to "11012", 2024 to "11011"), store.rows.mapTo(mutableSetOf()) { it.year to it.reprtCode })
    }

    @Test
    fun `백필 창을 넓히면 좁은 창으로 완료된 종목을 다시 백필한다`() {
        store.backfilled["005930"] = BackfillMarker(3, FinancialAccountMapper.MAPPING_VERSION)
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(disclosure(name = "사업보고서 (2021.12)", receipt = "20220310")),
            ),
        )
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = store,
            corps = FakeCorps(mapOf("005930" to "00126380")),
            runs = runs,
            meters = meters,
            backfillYears = 5,
            backfillPerRun = 10,
            requestInterval = Duration.ZERO,
            today = { today },
            pause = {},
        )

        val stored = job.syncOnce()

        assertEquals(1, stored)
        assertEquals(5, store.backfilled["005930"]?.windowYears)
    }

    @Test
    fun `매퍼 버전이 오르면 완료된 종목을 다시 백필한다 - 매핑 실패가 영구 확정되지 않는다`() {
        store.backfilled["005930"] = BackfillMarker(3, 1)
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(disclosure(name = "사업보고서 (2024.12)", receipt = "20250310")),
            ),
        )
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = store,
            corps = FakeCorps(mapOf("005930" to "00126380")),
            runs = runs,
            meters = meters,
            backfillPerRun = 10,
            mapperVersion = 2,
            requestInterval = Duration.ZERO,
            today = { today },
            pause = {},
        )

        val stored = job.syncOnce()

        assertEquals(1, stored)
        assertEquals(2, store.backfilled["005930"]?.mapperVersion)
    }

    @Test
    fun `축소된 매퍼로 재백필하면 더 이상 산출되지 않는 보고서의 기존 행을 지운다`() {
        store.backfilled["005930"] = BackfillMarker(3, 1)
        store.rows += historicalRow("005930", 2024)
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to listOf(DartFinancialAccount("IS", "dart_unknown", "지분법손익", null, 12))),
            corpDisclosures = mapOf(
                "00126380" to listOf(disclosure(name = "사업보고서 (2024.12)", receipt = "20250310")),
            ),
        )
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = store,
            corps = FakeCorps(mapOf("005930" to "00126380")),
            runs = runs,
            meters = meters,
            backfillPerRun = 10,
            mapperVersion = 2,
            requestInterval = Duration.ZERO,
            today = { today },
            pause = {},
        )

        job.syncOnce()

        assertTrue(store.rows.none { it.code == "005930" && it.year == 2024 })
        assertEquals(2, store.backfilled["005930"]?.mapperVersion)
        assertEquals(listOf("SUCCESS"), runs.finished)
    }

    @Test
    fun `정기공시가 없는 종목도 1회 시도 후 마커로 완료된다 - 스팩이 매 회차 재시도되지 않는다`() {
        val dart = StubDart(
            disclosures = emptyList(),
            accounts = emptyMap(),
            corpDisclosures = mapOf("00126380" to emptyList()),
        )

        val stored = job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertEquals(0, stored)
        assertEquals(listOf("SUCCESS"), runs.finished)
        assertTrue("005930" in store.backfilled)
        assertEquals(BackfillMarker(3, FinancialAccountMapper.MAPPING_VERSION), store.backfilled["005930"])
        assertTrue(store.rows.isEmpty())
    }

    @Test
    fun `보고서 하나가 재시도까지 실패하면 종목 통째로 미룬다 - 부분 적재는 공백을 영구히 숨긴다`() {
        val dart = object : StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(
                    disclosure(name = "사업보고서 (2025.12)", receipt = "20260310"),
                    disclosure(name = "분기보고서 (2026.03)", receipt = "20260515"),
                ),
            ),
        ) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                if (reprtCode == "11013") throw DartApiException("unknown", "timeout")
                return super.financialAccounts(corpCode, year, reprtCode, fsDiv)
            }
        }

        job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertTrue(store.rows.isEmpty())
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, runs.lastFailCount)
    }

    @Test
    fun `한 종목의 다중 보고서 실패는 스트릭 1이다 - 종목 하나가 브레이커로 뒤 종목을 막지 않는다`() {
        val corps = mapOf("000001" to "00000001", "000002" to "00000002")
        val dart = object : StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00000002:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00000001" to (1..6).map { n -> disclosure(name = "사업보고서 (202$n.12)", receipt = "20260310") },
                "00000002" to listOf(disclosure(name = "사업보고서 (2025.12)", receipt = "20260310")),
            ),
        ) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                if (corpCode == "00000001") throw DartApiException("unknown", "timeout")
                return super.financialAccounts(corpCode, year, reprtCode, fsDiv)
            }
        }

        val stored = job(
            dart,
            active = corps.keys,
            backfillPerRun = 10,
            failureStreakLimit = 3,
            corps = corps,
        ).syncOnce()

        assertEquals(1, stored)
        assertEquals("000002", store.rows.single().code)
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(0.0, meters.counter("batch.financials.breaker").count())
    }

    @Test
    fun `백필 종목 처리 중에도 데드라인을 확인한다 - 느린 DART가 락 임차를 넘기지 않게`() {
        var now = Instant.parse("2026-08-06T21:00:00Z")
        val dart = object : StubDart(
            disclosures = emptyList(),
            accounts = mapOf("00126380:CFS" to cfsAccounts),
            corpDisclosures = mapOf(
                "00126380" to listOf(
                    disclosure(name = "사업보고서 (2024.12)", receipt = "20250310"),
                    disclosure(name = "사업보고서 (2025.12)", receipt = "20260310"),
                ),
            ),
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
            universe = universe(setOf("005930")),
            store = store,
            corps = FakeCorps(mapOf("005930" to "00126380")),
            runs = runs,
            meters = meters,
            backfillPerRun = 10,
            requestInterval = Duration.ZERO,
            deadline = Duration.ofMinutes(100),
            clock = { now },
            today = { today },
            pause = {},
        )

        job.syncOnce()

        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1.0, meters.counter("batch.financials.deadline").count())
        assertTrue(store.rows.isEmpty())
    }

    @Test
    fun `인터럽트는 종목 실패로 흡수하지 않고 취소로 전파한다`() {
        var fetchCalls = 0
        val dart = object : StubDart(
            disclosures = emptyList(),
            accounts = emptyMap(),
            corpDisclosures = mapOf(
                "00000001" to listOf(disclosure(name = "사업보고서 (2024.12)", receipt = "20250310")),
                "00000002" to listOf(disclosure(name = "사업보고서 (2024.12)", receipt = "20250310")),
            ),
        ) {
            override fun financialAccounts(
                corpCode: String,
                year: Int,
                reprtCode: String,
                fsDiv: String,
            ): List<DartFinancialAccount> {
                fetchCalls += 1
                throw InterruptedException("shutdown")
            }
        }

        assertFailsWith<InterruptedException> {
            job(
                dart,
                active = setOf("000001", "000002"),
                backfillPerRun = 10,
                corps = mapOf("000001" to "00000001", "000002" to "00000002"),
            ).syncOnce()
        }

        assertTrue(Thread.interrupted())
        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, fetchCalls)
    }

    @Test
    fun `백필 목록 조회 실패는 잡을 FAILED로 남긴다 - 행이 없는 채로 남아 다음 회차가 재시도한다`() {
        val dart = object : StubDart(emptyList(), emptyMap()) {
            override fun periodicDisclosures(begin: LocalDate, end: LocalDate, corpCode: String?): List<DartDisclosure> {
                if (corpCode != null) throw DartApiException("unknown", "timeout")
                return super.periodicDisclosures(begin, end, corpCode)
            }
        }

        job(
            dart,
            active = setOf("005930"),
            backfillPerRun = 10,
            corps = mapOf("005930" to "00126380"),
        ).syncOnce()

        assertEquals(listOf("FAILED"), runs.finished)
        assertEquals(1, runs.lastFailCount)
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
            corps = FakeCorps(emptyMap()),
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
            corps = FakeCorps(emptyMap()),
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
            override fun completeBackfill(
                code: String,
                marker: BackfillMarker,
                rows: List<FinancialSummaryRow>,
                obsolete: List<ReportKey>,
                completedAt: Instant,
            ) = throw IllegalStateException("db down")
            override fun backfilledCodes(atLeast: BackfillMarker): Set<String> = emptySet()
        }
        val job = FinancialsSyncJob(
            dart = dart,
            universe = universe(setOf("005930")),
            store = brokenStore,
            corps = FakeCorps(emptyMap()),
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

        override fun hasIncompleteRun(job: String, runDate: String): Boolean = job in failedJobs
    }

    private class FakeFinancialStore : FinancialSummaryStore {
        val rows = mutableListOf<FinancialSummaryRow>()

        override fun upsert(row: FinancialSummaryRow) {
            rows += row
        }

        val backfilled = mutableMapOf<String, BackfillMarker>()

        override fun completeBackfill(
            code: String,
            marker: BackfillMarker,
            rows: List<FinancialSummaryRow>,
            obsolete: List<ReportKey>,
            completedAt: Instant,
        ) {
            this.rows.removeAll { row -> row.code == code && ReportKey(row.year, row.reprtCode) in obsolete.toSet() }
            this.rows += rows
            backfilled[code] = marker
        }

        override fun backfilledCodes(atLeast: BackfillMarker): Set<String> =
            backfilled.filterValues {
                it.windowYears >= atLeast.windowYears && it.mapperVersion >= atLeast.mapperVersion
            }.keys
    }
}
