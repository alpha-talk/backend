package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.sector.SectorDirectory
import org.springframework.stereotype.Component
import java.text.Normalizer

@Component
class StockEvidenceValidator(
    private val stocks: SectorDirectory,
) {
    fun accepts(verdict: StockVerdict, sourceCandidates: Set<String>, input: ClusterSummaryInput): Boolean {
        if (!verdict.relevant || verdict.relation != StockRelation.DIRECT) return false
        val aliases = stocks.stockAliases(verdict.code).map(::normalize).filter { it.length >= MIN_ALIAS_LENGTH }
        if (aliases.isEmpty()) return false
        if (verdict.code in sourceCandidates) return true

        val evidence = normalize(verdict.evidence)
        if (evidence.isEmpty() || aliases.none(evidence::contains)) return false
        return normalize(input.corpus()).contains(evidence)
    }

    private fun ClusterSummaryInput.corpus(): String =
        (listOf(repTitle) + articleTitles + listOfNotNull(body)).distinct().joinToString("\n")

    private fun normalize(value: String): String =
        Normalizer.normalize(value, Normalizer.Form.NFKC)
            .lowercase()
            .replace(WHITESPACE, " ")
            .trim()

    private companion object {
        const val MIN_ALIAS_LENGTH = 2
        val WHITESPACE = Regex("\\s+")
    }
}
