package com.alphatalk.worker.batch.financials

import kotlin.test.Test
import kotlin.test.assertEquals

class ReportReferenceTest {
    @Test
    fun `보고서명에서 사업연도와 보고서 코드를 판별한다`() {
        assertEquals(ReportReference(2025, "11011"), ReportReference.parse("사업보고서 (2025.12)"))
        assertEquals(ReportReference(2026, "11012"), ReportReference.parse("반기보고서 (2026.06)"))
        assertEquals(ReportReference(2026, "11013"), ReportReference.parse("분기보고서 (2026.03)"))
        assertEquals(ReportReference(2026, "11014"), ReportReference.parse("분기보고서 (2026.09)"))
    }

    @Test
    fun `기재정정 접두어가 붙어도 판별한다`() {
        assertEquals(ReportReference(2025, "11011"), ReportReference.parse("[기재정정]사업보고서 (2025.12)"))
    }

    @Test
    fun `정기보고서가 아니면 null`() {
        assertEquals(null, ReportReference.parse("주요사항보고서(유상증자결정)"))
        assertEquals(null, ReportReference.parse("사업보고서"))
    }
}
