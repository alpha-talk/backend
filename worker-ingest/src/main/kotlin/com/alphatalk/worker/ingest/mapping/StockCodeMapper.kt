package com.alphatalk.worker.ingest.mapping

interface StockCodeMapper {
    fun map(title: String, excerpt: String?): MappingResult
}

data class MappingResult(
    val codes: List<String>,
    val macroHint: String? = null,
) {
    val unmatched: Boolean get() = codes.isEmpty() && macroHint == null
}
