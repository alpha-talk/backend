package com.alphatalk.worker.batch.financials

data class ReportReference(
    val year: Int,
    val reprtCode: String,
) {
    companion object {
        const val Q1 = "11013"
        const val HALF = "11012"
        const val Q3 = "11014"
        const val ANNUAL = "11011"

        private val PATTERN = Regex("(사업보고서|반기보고서|분기보고서)\\s*\\((\\d{4})\\.(\\d{2})\\)")

        fun parse(reportName: String): ReportReference? {
            val match = PATTERN.find(reportName) ?: return null
            val (kind, year, month) = match.destructured
            val reprtCode = when (kind) {
                "사업보고서" -> ANNUAL
                "반기보고서" -> HALF
                else -> if (month.toInt() <= 6) Q1 else Q3
            }
            return ReportReference(year = year.toInt(), reprtCode = reprtCode)
        }
    }
}
