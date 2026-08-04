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

    private class FakeMinuteCandleStore(private val rows: List<MinuteCandleRow> = emptyList()) : MinuteCandleStore {
        var lastLimit = 0

        override fun findLatestUpTo(code: String, toDate: String?, toTime: String?, limit: Int): List<MinuteCandleRow> {
            lastLimit = limit
            return rows
                .filter { toDate == null || it.date < toDate || (it.date == toDate && it.time <= toTime!!) }
                .sortedWith(compareByDescending(MinuteCandleRow::date).thenByDescending(MinuteCandleRow::time))
                .take(limit)
        }
    }

    private class RecordingRefresher(private val failure: Throwable? = null) : MinuteCandleRefresher {
        val refreshed = mutableListOf<String>()

        override fun refresh(code: String) {
            refreshed += code
            failure?.let { throw it }
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
        minuteCandles: MinuteCandleStore = FakeMinuteCandleStore(),
        minuteRefresher: MinuteCandleRefresher = RecordingRefresher(),
        valuations: ValuationStore = FakeValuationStore(),
        financials: FinancialsStore = FakeFinancialsStore(),
        investors: InvestorFlowStore = FakeInvestorStore(),
    ) = StockInfoService(profiles, candles, minuteCandles, minuteRefresher, valuations, financials, investors)

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
    fun `적자 금액은 0 방향 절사가 아니라 내림으로 환산한다`() {
        val rows = listOf(
            FinancialRecord(2025, "11011", "CFS", -150_000_000, -50_000_000, null, null, null, null, Instant.parse("2026-04-01T00:00:00Z")),
        )

        val annual = service(financials = FakeFinancialsStore(rows)).financials("005930", 1).annual.single()

        assertEquals(-2, annual.revenue)
        assertEquals(-1, annual.operatingProfit)
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
            { it.candles("999999", "5m", null, null) },
            { it.valuation("999999") },
            { it.financials("999999", null) },
            { it.investors("999999", null) },
        ).forEach { call ->
            val e = assertFailsWith<ApiException> { call(service()) }
            assertEquals(ErrorCode.NOT_FOUND, e.code)
        }
    }

    private fun minuteRows(date: String, from: String, count: Int): List<MinuteCandleRow> {
        val start = from.take(2).toInt() * 60 + from.drop(2).toInt()
        return (0 until count).map { offset ->
            val minutes = start + offset
            MinuteCandleRow(
                date = date,
                time = "%02d%02d".format(minutes / 60, minutes % 60),
                open = 100,
                high = 110,
                low = 90,
                close = 105,
                volume = 10,
                value = 1000,
            )
        }
    }

    @Test
    fun `분봉 조회는 신선화를 트리거하고 5분 버킷을 오름차순으로 준다`() {
        val refresher = RecordingRefresher()
        val store = FakeMinuteCandleStore(minuteRows("20260804", "0900", 31))

        val response = service(minuteCandles = store, minuteRefresher = refresher)
            .candles("005930", "5m", 3, null)

        assertEquals(listOf("005930"), refresher.refreshed)
        assertEquals("5m", response.period)
        assertEquals(listOf("0920", "0925", "0930"), response.items.map(CandleView::time))
        assertEquals(listOf("20260804", "20260804", "20260804"), response.items.map(CandleView::date))
        assertEquals(5_000, response.items.first().value)
        assertEquals(50, response.items.first().volume)
        assertTrue(response.pageInfo.hasMoreBefore)
        assertEquals("202608040919", response.pageInfo.nextTo)
    }

    @Test
    fun `분봉 버킷은 일 경계를 넘지 않는다`() {
        val store = FakeMinuteCandleStore(
            minuteRows("20260803", "1520", 11) + minuteRows("20260804", "0900", 10),
        )

        val response = service(minuteCandles = store).candles("005930", "30m", 500, null)

        assertEquals(
            listOf("20260803" to "1500", "20260803" to "1530", "20260804" to "0900"),
            response.items.map { it.date to it.time },
        )
        assertEquals(false, response.pageInfo.hasMoreBefore)
        assertNull(response.pageInfo.nextTo)
    }

    @Test
    fun `마감 직후 count=1 조회도 20시 마감 행이 합산된 마지막 버킷을 돌려준다`() {
        val store = FakeMinuteCandleStore(minuteRows("20260804", "1930", 31))

        val response = service(minuteCandles = store).candles("005930", "5m", 1, null)

        assertEquals(listOf("1955"), response.items.map(CandleView::time))
        assertEquals(60, response.items.single().volume)
        assertEquals(6_000, response.items.single().value)
        assertTrue(response.pageInfo.hasMoreBefore)
        assertEquals("202608041954", response.pageInfo.nextTo)
    }

    @Test
    fun `프리마켓과 정규장 봉이 08시 그리드로 나뉘고 세션 공백은 버킷을 만들지 않는다`() {
        val store = FakeMinuteCandleStore(
            minuteRows("20260804", "0800", 10) + minuteRows("20260804", "0900", 10),
        )

        val response = service(minuteCandles = store).candles("005930", "60m", 500, null)

        assertEquals(listOf("0800", "0900"), response.items.map(CandleView::time))
        assertEquals(false, response.pageInfo.hasMoreBefore)
    }

    @Test
    fun `애프터마켓 봉도 정규장과 이어서 조회된다`() {
        val store = FakeMinuteCandleStore(
            minuteRows("20260804", "1525", 10),
        )

        val response = service(minuteCandles = store).candles("005930", "5m", 500, null)

        assertEquals(listOf("1525", "1530"), response.items.map(CandleView::time))
        assertEquals(50, response.items.last().volume)
    }

    @Test
    fun `희소 데이터가 저장분 전부여도 count 상한을 지킨다`() {
        val sparse = listOf("0900", "0930", "1000", "1030").map { time ->
            MinuteCandleRow("20260804", time, 100, 110, 90, 105, 10, 1000)
        }

        val response = service(minuteCandles = FakeMinuteCandleStore(sparse))
            .candles("005930", "5m", 2, null)

        assertEquals(listOf("1000", "1030"), response.items.map(CandleView::time))
        assertTrue(response.pageInfo.hasMoreBefore)
        assertEquals("202608040959", response.pageInfo.nextTo)
    }

    @Test
    fun `신선화 실패는 분봉 조회를 막지 않는다`() {
        val store = FakeMinuteCandleStore(minuteRows("20260804", "0900", 5))
        val refresher = RecordingRefresher(failure = IllegalStateException("worker down"))

        val response = service(minuteCandles = store, minuteRefresher = refresher)
            .candles("005930", "1m", 5, null)

        assertEquals(5, response.items.size)
        assertEquals(listOf("005930"), refresher.refreshed)
    }

    @Test
    fun `분봉의 to는 yyyyMMddHHmm 형식을 검증하고 커서 이전만 준다`() {
        val store = FakeMinuteCandleStore(minuteRows("20260804", "0900", 60))

        assertEquals(
            ErrorCode.VALIDATION_FAILED,
            assertFailsWith<ApiException> { service().candles("005930", "1m", null, "20260804") }.code,
        )

        val response = service(minuteCandles = store).candles("005930", "1m", 10, "202608040930")
        assertEquals("0930", response.items.last().time)
        assertEquals("0921", response.items.first().time)
        assertEquals("202608040920", response.pageInfo.nextTo)
    }
}
