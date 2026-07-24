package com.alphatalk.worker.ingest.mapping

class DictionaryStockCodeMapper(
    stocks: Map<String, List<String>>,
    private val macroKeywords: List<String>,
) : StockCodeMapper {

    private val namesByCode = stocks.mapValues { (_, names) -> names.filter { it.isNotBlank() } }

    override fun map(title: String, excerpt: String?): MappingResult {
        val text = if (excerpt.isNullOrBlank()) title else "$title $excerpt"
        val codes = namesByCode
            .filterValues { names -> names.any { text.contains(it) } }
            .keys.sorted()
        if (codes.isNotEmpty()) return MappingResult(codes)
        return MappingResult(emptyList(), macroKeywords.firstOrNull { text.contains(it) })
    }
}
