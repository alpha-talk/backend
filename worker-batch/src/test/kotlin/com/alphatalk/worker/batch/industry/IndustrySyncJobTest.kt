package com.alphatalk.worker.batch.industry

import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class IndustrySyncJobTest {

    private val ksic = KsicCatalog()

    @Test
    fun `KSIC 분류표는 자릿수별 항목을 모두 읽는다`() {
        val entries = ksic.entries()
        assertEquals(1196, entries.count { it.level == 5 })
        assertEquals(232, entries.count { it.level == 3 })
        assertEquals("전기회로 개폐, 보호 장치 제조업", entries.first { it.code == "28121" }.name)
    }

    @Test
    fun `그룹 코드는 앞 3자리로 맞추고 3자리 미만은 원본을 쓴다`() {
        assertEquals("281", ksic.groupCodeOf("28121"))
        assertEquals("264", ksic.groupCodeOf("264"))
        assertEquals("26", ksic.groupCodeOf("26"))
    }

    @Test
    fun `활성 종목만 조회하고 업종코드를 그룹으로 접어 저장한다`() {
        val store = RecordingStore(active = setOf("010120", "005930"))
        val dart = FakeDartClient(
            corps = listOf(
                DartCorp("00105855", "010120", "엘에스일렉트릭"),
                DartCorp("00126380", "005930", "삼성전자"),
                DartCorp("00999999", "999999", "비상장자회사"),
                DartCorp("00888888", null, "종목코드없음"),
            ),
            companies = mapOf(
                "00105855" to company("00105855", "010120", "28121"),
                "00126380" to company("00126380", "005930", "264"),
            ),
        )

        val stored = job(dart, store).syncOnce()

        assertEquals(2, stored)
        assertEquals(3, store.corpMap.size)
        assertTrue(store.corpMap.none { it.stockCode == null })
        assertEquals(2, dart.companyCalls)
        assertEquals("281", store.stockIndustries.first { it.code == "010120" }.groupCode)
        assertEquals("264", store.stockIndustries.first { it.code == "005930" }.groupCode)
        assertEquals(1196, store.industries.count { it.level == 5 })
    }

    @Test
    fun `상한을 넘는 그룹은 한 단계 아래 코드로 쪼갠다`() {
        val codes = (1..4).map { "01000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { DartCorp("corp$it", it, "회사$it") },
            companies = codes.mapIndexed { index, code ->
                "corp$code" to company("corp$code", code, if (index < 2) "28111" else "28121")
            }.toMap(),
        )

        job(dart, store, groupMaxSize = 3).syncOnce()

        assertEquals(setOf("2811", "2812"), store.stockIndustries.map { it.groupCode }.toSet())
    }

    @Test
    fun `상한 이내 그룹은 3자리를 유지한다`() {
        val codes = (1..4).map { "01000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { DartCorp("corp$it", it, "회사$it") },
            companies = codes.associate { "corp$it" to company("corp$it", it, "28121") },
        )

        job(dart, store, groupMaxSize = 10).syncOnce()

        assertEquals(setOf("281"), store.stockIndustries.map { it.groupCode }.toSet())
    }

    @Test
    fun `그룹 오버라이드는 DART 신고 업종을 덮어쓴다`() {
        val store = RecordingStore(active = setOf("005930"))
        val dart = FakeDartClient(
            corps = listOf(DartCorp("corp1", "005930", "삼성전자")),
            companies = mapOf("corp1" to company("corp1", "005930", "264")),
        )

        job(dart, store, overrides = mapOf("005930" to "261")).syncOnce()

        val record = store.stockIndustries.single()
        assertEquals("261", record.groupCode)
        assertEquals("264", record.indutyCode)
    }

    @Test
    fun `KSIC 표에 없는 업종코드는 상위 분류 이름으로 등록한다`() {
        val store = RecordingStore(active = setOf("010010"))
        val dart = FakeDartClient(
            corps = listOf(DartCorp("corp1", "010010", "한일사료")),
            companies = mapOf("corp1" to company("corp1", "010010", "109")),
        )

        job(dart, store).syncOnce()

        assertEquals("109", store.stockIndustries.single().groupCode)
        assertEquals("식료품 제조업", store.industries.last { it.code == "109" }.name)
    }

    @Test
    fun `업종코드가 없는 회사는 건너뛰고 실패로 집계한다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(
            corps = listOf(DartCorp("00105855", "010120", "엘에스일렉트릭")),
            companies = mapOf("00105855" to company("00105855", "010120", null)),
        )

        assertEquals(0, job(dart, store).syncOnce())
        assertTrue(store.stockIndustries.isEmpty())
        assertEquals(1, store.failCount)
    }

    @Test
    fun `조회 실패는 한 번 재시도한다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(
            corps = listOf(DartCorp("00105855", "010120", "엘에스일렉트릭")),
            companies = mapOf("00105855" to company("00105855", "010120", "28121")),
            failFirstCall = true,
        )

        assertEquals(1, job(dart, store).syncOnce())
        assertEquals(2, dart.companyCalls)
        assertEquals(0, store.failCount)
    }

    @Test
    fun `상장사가 없으면 실패로 기록하고 전파한다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(corps = listOf(DartCorp("00888888", null, "종목코드없음")), companies = emptyMap())

        assertFailsWith<IllegalStateException> { job(dart, store).syncOnce() }
        assertTrue(store.stockIndustries.isEmpty())
    }

    @Test
    fun `같은 날 성공한 실행은 건너뛴다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(
            corps = listOf(DartCorp("00105855", "010120", "엘에스일렉트릭")),
            companies = mapOf("00105855" to company("00105855", "010120", "28121")),
        )
        val runs = OnceOnlyRunStore()

        assertEquals(1, job(dart, store, runs).syncOnce())
        assertEquals(0, job(dart, store, runs).syncOnce())
        assertEquals(1, dart.companyCalls)
    }

    private fun job(
        dart: DartClient,
        store: RecordingStore,
        runs: BatchJobRunStore = OnceOnlyRunStore(),
        groupMaxSize: Int = 100,
        overrides: Map<String, String> = emptyMap(),
    ) = IndustrySyncJob(
        dart = dart,
        ksic = ksic,
        store = store,
        runs = runs.also { store.runs = it },
        meters = SimpleMeterRegistry(),
        requestInterval = Duration.ZERO,
        groupMaxSize = groupMaxSize,
        groupOverrides = overrides,
        today = { LocalDate.of(2026, 8, 5) },
        pause = {},
    )

    private fun company(corpCode: String, stockCode: String, induty: String?) = DartCompany(
        corpCode = corpCode,
        stockCode = stockCode,
        indutyCode = induty,
        corpName = "정식명칭",
        corpNameEng = "ENG NAME",
        stockName = "종목명",
        homepage = "www.example.com",
    )

    private class FakeDartClient(
        private val corps: List<DartCorp>,
        private val companies: Map<String, DartCompany>,
        private val failFirstCall: Boolean = false,
    ) : DartClient {
        var companyCalls = 0

        override fun corpCodes(): List<DartCorp> = corps

        override fun company(corpCode: String): DartCompany? {
            companyCalls += 1
            if (failFirstCall && companyCalls == 1) throw IllegalStateException("일시 실패")
            return companies[corpCode]
        }
    }

    private class RecordingStore(private val active: Set<String>) : IndustryStore {
        val industries = mutableListOf<KsicEntry>()
        val corpMap = mutableListOf<DartCorp>()
        val stockIndustries = mutableListOf<StockIndustryRecord>()
        var runs: BatchJobRunStore? = null

        val failCount: Int get() = (runs as? OnceOnlyRunStore)?.lastFailCount ?: 0

        override fun upsertIndustries(entries: List<KsicEntry>): Int {
            industries += entries
            return entries.size
        }

        override fun upsertCorpMap(corps: List<DartCorp>): Int {
            corpMap += corps
            return corps.size
        }

        override fun upsertStockIndustries(records: List<StockIndustryRecord>): Int {
            stockIndustries += records
            return records.size
        }

        override fun activeStockCodes(): Set<String> = active
    }

    private class OnceOnlyRunStore : BatchJobRunStore {
        private var succeeded = false
        var lastFailCount = 0

        override fun start(job: String, runDate: String, startedAt: Instant): Long? =
            if (succeeded) null else 1L

        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            succeeded = true
            lastFailCount = failCount
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            assertNull(null)
        }
    }
}
