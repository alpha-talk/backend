package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.sector.StockMasterEntity
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class MarketFactSheetAssemblerTest {
    private val stocks = listOf(
        StockMasterEntity(code = "005930", name = "삼성전자", sectorCode = "33"),
        StockMasterEntity(code = "000660", name = "SK하이닉스", sectorCode = "33"),
        StockMasterEntity(code = "373220", name = "LG에너지솔루션", sectorCode = "44"),
        StockMasterEntity(code = "105560", name = "KB금융", sectorCode = "27"),
    )
    private val sectorNames = mapOf("33" to "반도체", "44" to "2차전지", "27" to "은행")

    @Test
    fun `등락 분포와 업종 평균·수급 합계를 집계한다`() {
        val sheet = MarketFactSheetAssembler.assemble(
            date = "2026-07-16",
            activeStocks = stocks,
            sectorNames = sectorNames,
            currentCloses = mapOf("005930" to 90, "000660" to 110, "373220" to 120, "105560" to 100),
            previousCloses = mapOf("005930" to 100, "000660" to 100, "373220" to 100, "105560" to 100),
            flowRows = listOf(
                InvestorFlowReadEntity(code = "005930", date = "20260716", foreignNet = -500, institutionNet = 100),
                InvestorFlowReadEntity(code = "000660", date = "20260716", foreignNet = -300, institutionNet = 50),
                InvestorFlowReadEntity(code = "373220", date = "20260716", foreignNet = 900, institutionNet = -20),
            ),
        )

        assertEquals(2, sheet.advancers)
        assertEquals(1, sheet.decliners)
        assertEquals(1, sheet.unchanged)
        assertEquals("2차전지", sheet.topSectors.first().name)
        assertEquals(20.0, sheet.topSectors.first().avgChangePct)
        assertEquals("반도체", sheet.bottomSectors.first().name)
        assertEquals(0.0, sheet.bottomSectors.first().avgChangePct)
        assertEquals(2, sheet.bottomSectors.first().stockCount)
        assertEquals("2차전지" to 900L, sheet.foreignNetBuyTop.first().let { it.name to it.netBuy })
        assertEquals("반도체" to 150L, sheet.institutionNetBuyTop.first().let { it.name to it.netBuy })
    }

    @Test
    fun `전일 종가가 없는 종목은 집계에서 제외한다`() {
        val sheet = MarketFactSheetAssembler.assemble(
            date = "2026-07-16",
            activeStocks = stocks,
            sectorNames = sectorNames,
            currentCloses = mapOf("005930" to 110),
            previousCloses = mapOf("005930" to 100, "000660" to 100),
            flowRows = emptyList(),
        )

        assertEquals(1, sheet.advancers)
        assertEquals(0, sheet.decliners + sheet.unchanged)
        assertEquals(1, sheet.topSectors.single().stockCount)
    }
}
