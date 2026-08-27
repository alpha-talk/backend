package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.worker.llm.sector.SectorDirectory
import com.alphatalk.worker.llm.sector.SectorInfo
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class StockEvidenceValidatorTest {
    private val directory = object : SectorDirectory {
        override fun allSectors() = emptyList<SectorInfo>()
        override fun sectorName(sectorCode: String) = null
        override fun memberCodes(sectorCode: String) = emptyList<String>()
        override fun stockName(stockCode: String) = if (stockCode == "005930") "삼성전자" else null
        override fun stockAliases(stockCode: String) =
            if (stockCode == "005930") setOf("삼성전자", "삼전") else emptySet()
        override fun sectorOf(stockCode: String) = null
    }
    private val validator = StockEvidenceValidator(directory)
    private val input = ClusterSummaryInput(
        repTitle = "삼전, 차세대 HBM 공급",
        articleTitles = listOf("삼성전자 HBM4 공급 계약"),
        body = "삼성전자가 주요 고객사와 공급 계약을 체결했다.",
        stocks = emptyList(),
        sectors = emptyList(),
    )

    @Test
    fun `소스 후보 DIRECT는 실제 종목이면 기사 근거 없이도 채택한다`() {
        assertTrue(validator.accepts(verdict(evidence = ""), setOf("005930"), input))
    }

    @Test
    fun `후보 밖 DIRECT는 원문에 존재하는 정식명 근거를 요구한다`() {
        assertTrue(validator.accepts(verdict(evidence = "삼성전자 HBM4 공급 계약"), emptySet(), input))
        assertFalse(validator.accepts(verdict(evidence = "삼성전자 파운드리 수주"), emptySet(), input))
    }

    @Test
    fun `등록 별칭이 포함된 원문 근거도 채택한다`() {
        assertTrue(validator.accepts(verdict(evidence = "삼전, 차세대 HBM 공급"), emptySet(), input))
    }

    @Test
    fun `다른 회사 근거와 INDIRECT는 확신도가 높아도 거부한다`() {
        assertFalse(validator.accepts(verdict(evidence = "LG전자 신제품"), emptySet(), input))
        assertFalse(
            validator.accepts(
                verdict(evidence = "삼성전자 HBM4 공급 계약", relation = StockRelation.INDIRECT),
                emptySet(),
                input,
            ),
        )
    }

    @Test
    fun `stock master에 없는 코드는 소스 후보여도 거부한다`() {
        assertFalse(validator.accepts(verdict(code = "999999"), setOf("999999"), input))
    }

    private fun verdict(
        code: String = "005930",
        evidence: String = "삼성전자",
        relation: StockRelation = StockRelation.DIRECT,
    ) = StockVerdict(
        code = code,
        relevant = true,
        sentiment = Sentiment.POSITIVE,
        confidence = 0.99,
        reason = "직접 관련",
        relation = relation,
        evidence = evidence,
    )
}
