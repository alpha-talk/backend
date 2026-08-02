package com.alphatalk.coreapi.stockinfo

import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class StockInfoServiceTest {
    private class FakeProfileStore(
        private val profiles: Map<String, StockProfile> = mapOf(
            "005930" to StockProfile(
                code = "005930",
                name = "삼성전자",
                market = "KOSPI",
                sectorName = "전기전자",
                sharesOutstanding = 5_846_278_000,
                listedAt = LocalDate.of(1975, 6, 11),
                updatedAt = Instant.parse("2026-07-07T00:00:00Z"),
            ),
        ),
    ) : StockProfileStore {
        override fun findActive(code: String) = profiles[code]
        override fun existsActive(code: String) = profiles.containsKey(code)
    }

    private class FakeCandleStore(private val rows: List<DailyCandle> = emptyList()) : CandleStore {
        var lastLimit = 0
        var lastTo: String? = null

        override fun findLatestUpTo(code: String, toDate: String?, limit: Int): List<DailyCandle> {
            lastLimit = limit
            lastTo = toDate
            return rows.filter { toDate == null || it.date <= toDate }.take(limit)
        }
    }

    private class FakeValuationStore(private val record: ValuationRecord? = null) : ValuationStore {
        override fun findLatest(code: String) = record
    }

    private class FakeFinancialsStore(private val rows: List<FinancialRecord> = emptyList()) : FinancialsStore {
        override fun findAll(code: String) = rows
    }

    private class FakeInvestorStore(private val rows: List<InvestorFlowRecord> = emptyList()) : InvestorFlowStore {
        var lastDays = 0

        override fun findLatest(code: String, days: Int): List<InvestorFlowRecord> {
            lastDays = days
            return rows.take(days)
        }
    }

    private fun service(
        profiles: StockProfileStore = FakeProfileStore(),
        candles: CandleStore = FakeCandleStore(),
        valuations: ValuationStore = FakeValuationStore(),
        financials: FinancialsStore = FakeFinancialsStore(),
        investors: InvestorFlowStore = FakeInvestorStore(),
    ) = StockInfoService(profiles, candles, valuations, financials, investors)

    @Test
    fun `개요는 섹터 이름과 상장일을 함께 준다`() {
        val overview = service().overview("005930")

        assertEquals("삼성전자", overview.name)
        assertEquals("전기전자", overview.sector)
        assertEquals("19750611", overview.listedAt)
        assertEquals(Instant.parse("2026-07-07T00:00:00Z").toEpochMilli(), overview.updatedAt)
    }

    @Test
    fun `없는 종목의 개요는 404다`() {
        val e = assertFailsWith<ApiException> { service().overview("999999") }

        assertEquals(ErrorCode.NOT_FOUND, e.code)
    }

    @Test
    fun `종목 코드 형식이 틀리면 400이다`() {
        val e = assertFailsWith<ApiException> { service().overview("SAMSUNG") }

        assertEquals(ErrorCode.VALIDATION_FAILED, e.code)
    }

    @Test
    fun `밸류에이션은 시총을 억원으로 환산한다`() {
        val valuation = service(
            valuations = FakeValuationStore(
                ValuationRecord(
                    date = "20260706",
                    per = BigDecimal("12.3"),
                    pbr = BigDecimal("1.1"),
                    eps = 5800,
                    bps = 65000,
                    marketCapWon = 425_000_000_000_000,
                ),
            ),
        ).valuation("005930")

        assertEquals(12.3, valuation.per)
        assertEquals(4_250_000, valuation.marketCap)
        assertEquals("20260706", valuation.asOf)
    }

    @Test
    fun `밸류에이션이 아직 없으면 404다`() {
        val e = assertFailsWith<ApiException> { service().valuation("005930") }

        assertEquals(ErrorCode.NOT_FOUND, e.code)
    }

    @Test
    fun `재무는 연간과 분기를 나누고 금액을 억원으로 환산한다`() {
        val rows = listOf(
            FinancialRecord(2026, "11013", "CFS", 79_000_000_000_000, null, null, null, null, null, Instant.parse("2026-05-15T00:00:00Z")),
            FinancialRecord(2025, "11011", "CFS", 302_000_000_000_000, 35_000_000_000_000, 28_000_000_000_000, null, null, null, Instant.parse("2026-04-01T00:00:00Z")),
            FinancialRecord(2025, "11014", "CFS", 76_000_000_000_000, null, null, null, null, null, Instant.parse("2025-10-30T00:00:00Z")),
            FinancialRecord(2024, "11011", "CFS", 280_000_000_000_000, null, null, null, null, null, Instant.parse("2025-04-01T00:00:00Z")),
        )

        val financials = service(financials = FakeFinancialsStore(rows)).financials("005930", 3)

        assertEquals(listOf("2025", "2024"), financials.annual.map(FinancialRow::period))
        assertEquals(3_020_000, financials.annual.first().revenue)
        assertEquals(350_000, financials.annual.first().operatingProfit)
        assertEquals("20260401", financials.annual.first().asOf)
        assertEquals(listOf("2026Q1", "2025Q3"), financials.quarterly.map(FinancialRow::period))
        assertEquals("DART", financials.quarterly.first().source)
    }

    @Test
    fun `재무가 없으면 빈 목록으로 200이다`() {
        val financials = service().financials("005930", null)

        assertTrue(financials.annual.isEmpty())
        assertTrue(financials.quarterly.isEmpty())
    }

    @Test
    fun `수급은 기본 20일을 최신순으로 준다`() {
        val store = FakeInvestorStore(
            (1..30).map {
                InvestorFlowRecord("202607%02d".format(31 - it), -1000L * it, 800L * it, 200L * it)
            },
        )

        val investors = service(investors = store).investors("005930", null)

        assertEquals(20, store.lastDays)
        assertEquals(20, investors.items.size)
        assertEquals("20260730", investors.items.first().date)
    }

    @Test
    fun `봉 조회는 기간 토큰과 상한을 검증한다`() {
        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().candles("005930", "Y", null, null) }.code,
        )
        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().candles("005930", "D", 501, null) }.code,
        )
        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().candles("005930", "D", null, "2026-07-07") }.code,
        )
    }

    @Test
    fun `봉 조회는 to 이전을 오름차순으로 준다`() {
        val candles = FakeCandleStore(
            listOf(
                DailyCandle("20260709", 105, 108, 104, 107, 10, 100),
                DailyCandle("20260708", 103, 106, 102, 105, 10, 100),
                DailyCandle("20260707", 101, 104, 100, 103, 10, 100),
            ),
        )

        val response = service(candles = candles).candles("005930", null, 2, "20260708")

        assertEquals("D", response.period)
        assertEquals(listOf("20260707", "20260708"), response.items.map(CandleView::date))
        assertEquals("20260708", candles.lastTo)
        assertNull(response.items.firstOrNull { it.value != 100L })
    }

    @Test
    fun `없는 종목의 지표는 전부 404다`() {
        listOf<(StockInfoService) -> Unit>(
            { it.candles("999999", null, null, null) },
            { it.valuation("999999") },
            { it.financials("999999", null) },
            { it.investors("999999", null) },
        ).forEach { call ->
            val e = assertFailsWith<ApiException> { call(service()) }
            assertEquals(ErrorCode.NOT_FOUND, e.code)
        }
    }
}
