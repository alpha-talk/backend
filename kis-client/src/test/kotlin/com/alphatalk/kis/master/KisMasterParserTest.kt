package com.alphatalk.kis.master

import java.time.LocalDate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class KisMasterParserTest {
    private fun fixture(name: String): ByteArray =
        javaClass.getResourceAsStream("/fixtures/$name")!!.readBytes()

    private val kospi by lazy { KisMasterParser.parseAll(KisMarket.KOSPI, fixture("kospi_master_sample.mst")) }
    private val kosdaq by lazy { KisMasterParser.parseAll(KisMarket.KOSDAQ, fixture("kosdaq_master_sample.mst")) }

    @Test
    fun `KOSPI 종목의 코드 이름 업종 상장일을 읽는다`() {
        val samsung = kospi.single { it.code == "005930" }

        assertEquals("삼성전자", samsung.name)
        assertEquals(KisMarket.KOSPI, samsung.market)
        assertEquals("ST", samsung.groupCode)
        assertEquals("0027", samsung.sectorCode)
        assertEquals(LocalDate.of(1975, 6, 11), samsung.listedAt)
    }

    @Test
    fun `상장주수는 천주 단위를 주 단위로 환산한다`() {
        assertEquals(5_846_278_000L, kospi.single { it.code == "005930" }.sharesOutstanding)
        assertEquals(712_702_000L, kospi.single { it.code == "000660" }.sharesOutstanding)
        assertEquals(27_931_000L, kospi.single { it.code == "000020" }.sharesOutstanding)
    }

    @Test
    fun `주권과 그 외 상품을 그룹코드로 구분한다`() {
        assertTrue(kospi.single { it.code == "005930" }.isCommonStock)

        val etf = kospi.single { it.groupCode == "EF" }
        assertFalse(etf.isCommonStock)
    }

    @Test
    fun `미분류 업종은 코드가 아니라 없음으로 읽는다`() {
        assertNull(kospi.single { it.groupCode == "EF" }.sectorCode)
        assertNull(kosdaq.single { it.code == "0001A0" }.sectorCode)
    }

    @Test
    fun `KOSDAQ은 KOSPI와 다른 오프셋으로 읽는다`() {
        val row = kosdaq.single { it.code == "000440" }

        assertEquals("중앙에너비스", row.name)
        assertEquals(KisMarket.KOSDAQ, row.market)
        assertEquals("1011", row.sectorCode)
        assertEquals(6_227_000L, row.sharesOutstanding)
        assertEquals(LocalDate.of(1993, 3, 24), row.listedAt)
    }

    @Test
    fun `줄 길이가 다른 입력은 건너뛴다`() {
        val broken = "짧은 줄\n".toByteArray()

        assertTrue(KisMasterParser.parseAll(KisMarket.KOSPI, broken).isEmpty())
        assertNull(KisMasterParser.parseLine(KisMarket.KOSPI, broken))
    }

    @Test
    fun `KOSPI 표본 전체가 빠짐없이 파싱된다`() {
        assertEquals(4, kospi.size)
        assertEquals(3, kosdaq.size)
        assertTrue(kospi.all { it.code.isNotEmpty() && it.name.isNotEmpty() })
    }
}
