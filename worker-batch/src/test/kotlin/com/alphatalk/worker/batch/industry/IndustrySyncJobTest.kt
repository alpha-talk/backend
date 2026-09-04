package com.alphatalk.worker.batch.industry

import com.alphatalk.worker.batch.job.BatchJobRunStore
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class IndustrySyncJobTest {

    private val ksic = KsicCatalog()

    @Test
    fun `KSIC 분류표는 자릿수별 항목과 상위 코드를 함께 읽는다`() {
        val entries = ksic.entries()
        assertEquals(1196, entries.count { it.level == 5 })
        assertEquals(232, entries.count { it.level == 3 })
        val leaf = entries.first { it.code == "28121" }
        assertEquals("전기회로 개폐, 보호 장치 제조업", leaf.name)
        assertEquals("2812", leaf.parentCode)
        assertEquals(null, entries.first { it.code == "28" }.parentCode)
    }

    @Test
    fun `업종코드는 요청한 자릿수로 접고 더 짧으면 원본을 쓴다`() {
        assertEquals("281", ksic.sectorCodeOf("28121", 3))
        assertEquals("2812", ksic.sectorCodeOf("28121", 4))
        assertEquals("264", ksic.sectorCodeOf("264", 4))
        assertEquals("26", ksic.sectorCodeOf("26", 3))
    }

    @Test
    fun `활성 종목만 조회하고 소분류로 접어 저장한다`() {
        val store = RecordingStore(active = setOf("010120", "005930"))
        val dart = FakeDartClient(
            corps = listOf(
                corp("00105855", "010120"),
                corp("00126380", "005930"),
                corp("00999999", "999999"),
                corp("00888888", null),
            ),
            companies = mapOf(
                "00105855" to company("010120", "28121"),
                "00126380" to company("005930", "264"),
            ),
        )

        val stored = job(dart, store).syncOnce()

        assertEquals(2, stored)
        assertEquals(3, store.corpMap.size)
        assertTrue(store.corpMap.none { it.stockCode == null })
        assertEquals(2, dart.companyCalls)
        assertEquals("281", store.sectorOf("010120"))
        assertEquals("264", store.sectorOf("005930"))
        assertTrue(store.sectors.any { it.code == "281" })
    }

    @Test
    fun `일시적 조회 실패 종목은 기존 업종코드를 유지하고 그룹 계산에 포함된다`() {
        val store = RecordingStore(
            active = setOf("000001", "000002", "000003"),
            existingInduty = mapOf("000001" to "28111", "000002" to "28121", "000003" to "28121"),
        )
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002"), corp("c3", "000003")),
            companies = mapOf("c1" to company("000001", "28111")),
            ioErrorFor = setOf("c2", "c3"),
        )

        job(dart, store, groupMaxSize = 2, maxFailureRatio = 1.0).syncOnce()

        assertEquals(3, store.stockIndustries.size)
        assertEquals(setOf("2811", "2812"), store.stockIndustries.map { it.sectorCode }.toSet())
        assertEquals("28121", store.stockIndustries.single { it.code == "000003" }.indutyCode)
    }

    @Test
    fun `상한 이내면 소분류를 유지한다`() {
        val codes = (1..4).map { "01000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { corp("corp$it", it) },
            companies = codes.associate { "corp$it" to company(it, "28121") },
        )

        job(dart, store, groupMaxSize = 10).syncOnce()

        assertEquals(setOf("281"), store.stockIndustries.map { it.sectorCode }.toSet())
    }

    @Test
    fun `소분류로 부족하면 세세분류까지 단계적으로 쪼갠다`() {
        val store = RecordingStore(active = setOf("000001", "000002"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c1" to company("000001", "28121"), "c2" to company("000002", "28122")),
        )

        job(dart, store, groupMaxSize = 1).syncOnce()

        assertEquals(setOf("28121", "28122"), store.stockIndustries.map { it.sectorCode }.toSet())
    }

    @Test
    fun `세세분류까지 같은 코드면 상한을 넘어도 배정을 유지한다`() {
        val codes = (1..3).map { "00000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { corp("corp$it", it) },
            companies = codes.associate { "corp$it" to company(it, "28121") },
        )

        assertEquals(3, job(dart, store, groupMaxSize = 2).syncOnce())
        assertEquals(setOf("28121"), store.stockIndustries.map { it.sectorCode }.toSet())
    }

    @Test
    fun `더 쪼갤 수 없는 짧은 업종코드는 상한을 넘어도 잡을 실패시키지 않는다`() {
        val codes = (1..3).map { "00000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { corp("corp$it", it) },
            companies = codes.associate { "corp$it" to company(it, "264") },
        )

        assertEquals(3, job(dart, store, groupMaxSize = 2).syncOnce())
        assertEquals(setOf("264"), store.stockIndustries.map { it.sectorCode }.toSet())
    }

    @Test
    fun `오버라이드로 쪼갤 수 있는 그룹이 상한을 넘으면 잡을 실패시킨다`() {
        val store = RecordingStore(active = setOf("000001", "000002", "000003"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002"), corp("c3", "000003")),
            companies = mapOf(
                "c1" to company("000001", "28111"),
                "c2" to company("000002", "28121"),
                "c3" to company("000003", "28122"),
            ),
        )

        assertFailsWith<IllegalStateException> {
            job(dart, store, groupMaxSize = 1, overrides = mapOf("000002" to "281", "000003" to "281")).syncOnce()
        }
    }

    @Test
    fun `오버라이드는 DART 신고 업종을 덮되 원본을 보존한다`() {
        val store = RecordingStore(active = setOf("005930"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "005930")),
            companies = mapOf("c1" to company("005930", "264")),
        )

        job(dart, store, overrides = mapOf("005930" to "261")).syncOnce()

        val record = store.stockIndustries.single()
        assertEquals("261", record.sectorCode)
        assertEquals("264", record.indutyCode)
    }

    @Test
    fun `KSIC 표에 없는 업종코드는 상위 분류 이름으로 등록한다`() {
        val store = RecordingStore(active = setOf("010010"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "010010")),
            companies = mapOf("c1" to company("010010", "109")),
        )

        job(dart, store).syncOnce()

        assertEquals("109", store.sectorOf("010010"))
        assertEquals("식료품 제조업", store.sectors.last { it.code == "109" }.name)
    }

    @Test
    fun `업종코드를 한 번도 못 받은 종목은 배정 대상에서 빠진다`() {
        val store = RecordingStore(active = setOf("000001", "000002"), existingInduty = mapOf("000001" to "28121"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c1" to company("000001", "28121")),
            noDataFor = setOf("c2"),
        )

        job(dart, store).syncOnce()

        assertEquals(listOf("000002"), store.cleared)
    }

    @Test
    fun `운영 오류 상태는 전파해 잡을 실패시킨다`() {
        val store = RecordingStore(active = setOf("000001"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001")),
            companies = emptyMap(),
            errorFor = mapOf("c1" to "020"),
        )
        val runs = OnceOnlyRunStore()

        assertFailsWith<DartApiException> { job(dart, store, runs).syncOnce() }
        assertEquals(null, runs.succeededOk)
        assertEquals(1L, runs.failedId)
    }

    @Test
    fun `조회 실패는 한 번 재시도한다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "010120")),
            companies = mapOf("c1" to company("010120", "28121")),
            transientFailFirstCall = true,
        )

        assertEquals(1, job(dart, store, maxFailureRatio = 1.0).syncOnce())
        assertEquals(2, dart.companyCalls)
    }

    @Test
    fun `상장사가 없으면 실패로 기록하고 전파한다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(corps = listOf(corp("c1", null)), companies = emptyMap())

        assertFailsWith<IllegalStateException> { job(dart, store).syncOnce() }
        assertTrue(store.stockIndustries.isEmpty())
    }

    @Test
    fun `네트워크 오류도 조회 실패로 흡수한다`() {
        val store = RecordingStore(active = setOf("000001", "000002"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c1" to company("000001", "28121")),
            ioErrorFor = setOf("c2"),
        )

        assertEquals(1, job(dart, store, maxFailureRatio = 0.5).syncOnce())
        assertEquals("281", store.sectorOf("000001"))
    }

    @Test
    fun `조회 실패가 허용치를 넘으면 부분 결과를 저장하지 않는다`() {
        val store = RecordingStore(active = setOf("000001", "000002"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c1" to company("000001", "28121")),
            ioErrorFor = setOf("c2"),
        )
        val runs = OnceOnlyRunStore()

        assertFailsWith<IllegalStateException> { job(dart, store, runs, maxFailureRatio = 0.1).syncOnce() }
        assertTrue(store.stockIndustries.isEmpty())
        assertEquals(1L, runs.failedId)
    }

    @Test
    fun `DART에서 사라진 종목은 기존 업종코드까지 지운다`() {
        val store = RecordingStore(
            active = setOf("000001", "000002"),
            existingInduty = mapOf("000001" to "28121", "000002" to "28121"),
        )
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c1" to company("000001", "28121")),
            noDataFor = setOf("c2"),
        )

        job(dart, store).syncOnce()

        assertEquals(listOf("000002"), store.cleared)
        assertTrue(store.stockIndustries.none { it.code == "000002" })
    }

    @Test
    fun `같은 날 성공한 실행은 건너뛴다`() {
        val store = RecordingStore(active = setOf("010120"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "010120")),
            companies = mapOf("c1" to company("010120", "28121")),
        )
        val runs = OnceOnlyRunStore()

        assertEquals(1, job(dart, store, runs).syncOnce())
        assertEquals(0, job(dart, store, runs).syncOnce())
        assertEquals(1, dart.companyCalls)
    }

    @Test
    fun `데드라인에 도달하면 저장하지 않고 FAILED로 남긴다 - 느린 DART가 락 임차를 넘기지 않게`() {
        var now = Instant.parse("2026-08-04T21:30:00Z")
        val store = RecordingStore(active = setOf("000001", "000002", "000003"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002"), corp("c3", "000003")),
            companies = mapOf(
                "c1" to company("000001", "28121"),
                "c2" to company("000002", "28121"),
                "c3" to company("000003", "28121"),
            ),
            onCompany = { now = now.plusSeconds(46 * 60) },
        )
        val runs = OnceOnlyRunStore()
        val meters = SimpleMeterRegistry()

        val stored = job(dart, store, runs, meters = meters, deadline = Duration.ofMinutes(90), clock = { now }).syncOnce()

        assertEquals(0, stored)
        assertEquals(2, dart.companyCalls)
        assertTrue(store.stockIndustries.isEmpty())
        assertEquals(1L, runs.failedId)
        assertEquals(1, runs.failedCount)
        assertEquals(1.0, meters.counter("batch.industry.deadline").count())
    }

    @Test
    fun `HTTP 429·5xx와 DART 900은 종목 실패로 흡수하지 않고 즉시 잡을 실패시킨다`() {
        listOf("429", "503", "900").forEach { status ->
            val store = RecordingStore(active = setOf("000001", "000002"))
            val dart = FakeDartClient(
                corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
                companies = mapOf("c2" to company("000002", "28121")),
                errorFor = mapOf("c1" to status),
            )
            val runs = OnceOnlyRunStore()

            assertFailsWith<DartApiException>(status) { job(dart, store, runs, maxFailureRatio = 1.0).syncOnce() }
            assertEquals(1, dart.companyCalls, status)
            assertEquals(1L, runs.failedId, status)
            assertTrue(store.stockIndustries.isEmpty(), status)
        }
    }

    @Test
    fun `연속 실패가 한도에 닿으면 회차를 중단한다 - 전면 장애가 전 종목 호출을 소모하지 않게`() {
        val codes = (1..8).map { "00000$it" }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { corp("corp$it", it) },
            companies = emptyMap(),
            ioErrorFor = codes.map { "corp$it" }.toSet(),
        )
        val runs = OnceOnlyRunStore()
        val meters = SimpleMeterRegistry()

        val stored = job(dart, store, runs, meters = meters, maxFailureRatio = 1.0, failureStreakLimit = 3).syncOnce()

        assertEquals(0, stored)
        assertEquals(3, dart.companyCalls)
        assertTrue(store.stockIndustries.isEmpty())
        assertEquals(1L, runs.failedId)
        assertEquals(8, runs.failedCount)
        assertEquals(1.0, meters.counter("batch.industry.breaker").count())
    }

    @Test
    fun `성공이나 데이터 없음이 끼면 스트릭이 리셋된다 - 산발 실패는 브레이커를 열지 않는다`() {
        val store = RecordingStore(active = setOf("000001", "000002", "000003", "000004"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002"), corp("c3", "000003"), corp("c4", "000004")),
            companies = mapOf("c2" to company("000002", "28121")),
            noDataFor = setOf("c4"),
            ioErrorFor = setOf("c1", "c3"),
        )
        val runs = OnceOnlyRunStore()

        val stored = job(dart, store, runs, maxFailureRatio = 1.0, failureStreakLimit = 2).syncOnce()

        assertEquals(1, stored)
        assertEquals(6, dart.companyCalls)
        assertEquals(1, runs.succeededOk)
    }

    @Test
    fun `재시도 순회는 브레이커를 열지 않는다 - 산발 실패를 몰아 재시도해도 연속 실패로 보지 않는다`() {
        val codes = (1..12).map { "%06d".format(it) }
        val broken = codes.filterIndexed { index, _ -> index % 2 == 0 }
        val store = RecordingStore(active = codes.toSet())
        val dart = FakeDartClient(
            corps = codes.map { corp("corp$it", it) },
            companies = codes.filterNot { it in broken }.associate { "corp$it" to company(it, "28121") },
            ioErrorFor = broken.map { "corp$it" }.toSet(),
        )
        val runs = OnceOnlyRunStore()
        val meters = SimpleMeterRegistry()

        val stored = job(dart, store, runs, meters = meters, maxFailureRatio = 1.0, failureStreakLimit = 5).syncOnce()

        assertEquals(6, stored)
        assertEquals(18, dart.companyCalls)
        assertEquals(6, runs.succeededOk)
        assertEquals(0.0, meters.counter("batch.industry.breaker").count())
        assertEquals(6.0, meters.counter("batch.industry.failed").count())
    }

    @Test
    fun `인터럽트는 종목 실패로 흡수하지 않고 취소로 전파한다`() {
        val store = RecordingStore(active = setOf("000001", "000002"))
        val dart = FakeDartClient(
            corps = listOf(corp("c1", "000001"), corp("c2", "000002")),
            companies = mapOf("c2" to company("000002", "28121")),
            interruptFor = setOf("c1"),
        )
        val runs = OnceOnlyRunStore()

        assertFailsWith<InterruptedException> { job(dart, store, runs, maxFailureRatio = 1.0).syncOnce() }

        assertTrue(Thread.interrupted())
        assertEquals(1, dart.companyCalls)
        assertEquals(1L, runs.failedId)
    }

    private fun job(
        dart: DartClient,
        store: RecordingStore,
        runs: BatchJobRunStore = OnceOnlyRunStore(),
        groupMaxSize: Int = 100,
        overrides: Map<String, String> = emptyMap(),
        maxFailureRatio: Double = 0.05,
        meters: MeterRegistry = SimpleMeterRegistry(),
        failureStreakLimit: Int = 5,
        deadline: Duration = Duration.ofMinutes(90),
        clock: () -> Instant = Instant::now,
    ) = IndustrySyncJob(
        dart = dart,
        ksic = ksic,
        store = store,
        runs = runs,
        meters = meters,
        requestInterval = Duration.ZERO,
        groupMaxSize = groupMaxSize,
        groupOverrides = overrides,
        maxFailureRatio = maxFailureRatio,
        failureStreakLimit = failureStreakLimit,
        deadline = deadline,
        clock = clock,
        today = { LocalDate.of(2026, 8, 5) },
        pause = {},
    )

    private fun corp(corpCode: String, stockCode: String?) =
        DartCorp(corpCode, stockCode, "회사$corpCode", "20260805")

    private fun company(stockCode: String, induty: String?) = DartCompany(
        corpCode = "corp",
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
        private val noDataFor: Set<String> = emptySet(),
        private val errorFor: Map<String, String> = emptyMap(),
        private val ioErrorFor: Set<String> = emptySet(),
        private val interruptFor: Set<String> = emptySet(),
        private val transientFailFirstCall: Boolean = false,
        private val onCompany: () -> Unit = {},
    ) : DartClient {
        var companyCalls = 0

        override fun corpCodes(): List<DartCorp> = corps

        override fun periodicDisclosures(
            begin: java.time.LocalDate,
            end: java.time.LocalDate,
            corpCode: String?,
        ): List<DartDisclosure> = emptyList()

        override fun financialAccounts(
            corpCode: String,
            year: Int,
            reprtCode: String,
            fsDiv: String,
        ): List<DartFinancialAccount> = emptyList()

        override fun company(corpCode: String): DartCompany? {
            companyCalls += 1
            onCompany()
            if (transientFailFirstCall && companyCalls == 1) throw DartApiException("013", "일시 실패")
            errorFor[corpCode]?.let { throw DartApiException(it, "운영 오류") }
            if (corpCode in ioErrorFor) throw java.io.IOException("connection reset")
            if (corpCode in interruptFor) throw InterruptedException("shutdown")
            if (corpCode in noDataFor) return null
            return companies[corpCode]
        }
    }

    private class RecordingStore(
        private val active: Set<String>,
        private val existingInduty: Map<String, String> = emptyMap(),
    ) : IndustryStore {
        val sectors = mutableListOf<KsicEntry>()
        val corpMap = mutableListOf<DartCorp>()
        val stockIndustries = mutableListOf<StockIndustryRecord>()
        val cleared = mutableListOf<String>()

        fun sectorOf(code: String): String? = stockIndustries.firstOrNull { it.code == code }?.sectorCode

        override fun upsertSectors(entries: List<KsicEntry>): Int {
            sectors += entries
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

        override fun clearIndustryAssignments(codes: Collection<String>): Int {
            cleared += codes
            return codes.size
        }

        override fun activeStockCodes(): Set<String> = active

        override fun activeIndutyCodes(): Map<String, String> = existingInduty
    }

    private class OnceOnlyRunStore : BatchJobRunStore {
        private var succeeded = false
        var succeededOk: Int? = null
        var failedId: Long? = null
        var failedCount: Int? = null

        override fun start(job: String, runDate: String, startedAt: Instant): Long? = if (succeeded) null else 1L

        override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L

        override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {
            succeeded = true
            succeededOk = okCount
        }

        override fun fail(id: Long, error: String, finishedAt: Instant) {
            failedId = id
        }

        override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {
            failedId = id
            failedCount = failCount
        }
    }
}
