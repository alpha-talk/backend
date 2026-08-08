package com.alphatalk.worker.batch.financials

import com.alphatalk.worker.batch.industry.DartFinancialAccount
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FinancialAccountMapperTest {
    private fun account(
        sjDiv: String,
        accountId: String?,
        accountName: String,
        amount: Long?,
        accountDetail: String? = null,
    ) = DartFinancialAccount(sjDiv, accountId, accountName, accountDetail, amount)

    @Test
    fun `표준 account_id로 여섯 지표를 매핑한다`() {
        val figures = FinancialAccountMapper.map(
            listOf(
                account("BS", "ifrs-full_Assets", "자산총계", 4000),
                account("BS", "ifrs-full_Liabilities", "부채총계", 1500),
                account("BS", "ifrs-full_Equity", "자본총계", 2500),
                account("IS", "ifrs-full_Revenue", "수익(매출액)", 3000),
                account("IS", "dart_OperatingIncomeLoss", "영업이익", 350),
                account("IS", "ifrs-full_ProfitLoss", "당기순이익", 280),
            ),
        )

        assertEquals(FinancialFigures(3000, 350, 280, 4000, 1500, 2500), figures)
    }

    @Test
    fun `account_id가 비표준이면 계정과목명으로 폴백한다`() {
        val figures = FinancialAccountMapper.map(
            listOf(
                account("BS", "dart_custom1", "자산총계", 4000),
                account("CIS", null, "영업수익", 3000),
                account("CIS", null, "분기순이익(손실)", -50),
            ),
        )

        assertEquals(4000, figures.assets)
        assertEquals(3000, figures.revenue)
        assertEquals(-50, figures.netIncome)
    }

    @Test
    fun `세부 내역 행과 다른 재무제표 구분은 무시한다`() {
        val figures = FinancialAccountMapper.map(
            listOf(
                account("BS", "ifrs-full_Assets", "자산총계", 999, accountDetail = "유동자산"),
                account("CF", "ifrs-full_Revenue", "매출액", 777),
            ),
        )

        assertTrue(figures.isEmpty)
    }

    @Test
    fun `KRW가 아닌 통화의 계정은 원화 컬럼에 싣지 않는다`() {
        val figures = FinancialAccountMapper.map(
            listOf(
                DartFinancialAccount("BS", "ifrs-full_Assets", "자산총계", null, 4000, "USD"),
                DartFinancialAccount("IS", "ifrs-full_Revenue", "매출액", null, 3000, "KRW"),
            ),
        )

        assertEquals(null, figures.assets)
        assertEquals(3000, figures.revenue)
    }

    @Test
    fun `매핑 불가 계정만 있으면 isEmpty`() {
        val figures = FinancialAccountMapper.map(
            listOf(account("IS", "dart_unknown", "지분법손익", 12)),
        )

        assertTrue(figures.isEmpty)
    }
}
