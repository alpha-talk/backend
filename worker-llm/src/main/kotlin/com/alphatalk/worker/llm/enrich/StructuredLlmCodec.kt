package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

internal object StructuredLlmCodec {
    val mapper: ObjectMapper = jacksonObjectMapper()

    val summarySchema: Map<String, Any> = mapOf(
        "type" to "object",
        "additionalProperties" to false,
        "properties" to mapOf(
            "summary" to mapOf("type" to "string", "description" to "3줄 요약, 각 줄 80자 이내"),
            "marketRelevant" to mapOf(
                "type" to "boolean",
                "description" to "한국 증시 또는 상장사에 실질적 영향이 있는 기사인지 여부",
            ),
            "scope" to mapOf("type" to "string", "enum" to listOf("STOCK", "SECTOR", "MARKET")),
            "stocks" to mapOf(
                "type" to "array",
                "items" to mapOf(
                    "type" to "object",
                    "additionalProperties" to false,
                    "properties" to mapOf(
                        "code" to mapOf("type" to "string"),
                        "relevant" to mapOf("type" to "boolean"),
                        "sentiment" to mapOf(
                            "type" to "string",
                            "enum" to listOf("POSITIVE", "NEGATIVE", "NEUTRAL"),
                        ),
                        "confidence" to mapOf("type" to "number"),
                        "reason" to mapOf("type" to "string"),
                    ),
                    "required" to listOf("code", "relevant", "sentiment", "confidence", "reason"),
                ),
            ),
            "sectors" to mapOf(
                "type" to "array",
                "items" to mapOf(
                    "type" to "object",
                    "additionalProperties" to false,
                    "properties" to mapOf(
                        "sectorCode" to mapOf("type" to "string"),
                        "sentiment" to mapOf(
                            "type" to "string",
                            "enum" to listOf("POSITIVE", "NEGATIVE", "NEUTRAL"),
                        ),
                        "impact" to mapOf(
                            "type" to "string",
                            "enum" to listOf("HIGH", "MEDIUM", "LOW"),
                            "description" to IMPACT_CRITERIA,
                        ),
                        "confidence" to mapOf("type" to "number"),
                        "reason" to mapOf("type" to "string"),
                    ),
                    "required" to listOf("sectorCode", "sentiment", "impact", "confidence", "reason"),
                ),
            ),
        ),
        "required" to listOf("summary", "marketRelevant", "scope", "stocks", "sectors"),
    )

    val digestSchema: Map<String, Any> = mapOf(
        "type" to "object",
        "additionalProperties" to false,
        "properties" to mapOf(
            "title" to mapOf("type" to "string"),
            "summary" to mapOf("type" to "string", "description" to "종합 3줄"),
        ),
        "required" to listOf("title", "summary"),
    )

    fun summaryPrompt(input: ClusterSummaryInput): String = buildString {
        appendLine("다음 뉴스를 3줄로 요약하고 영향받는 종목·섹터를 판정하라. 투자 조언이 아니라 정보 요약이다.")
        appendLine("대표 제목: ${input.repTitle}")
        appendLine("클러스터 기사 제목들: ${input.articleTitles.joinToString(" | ")}")
        input.body?.let { appendLine("본문: ${it.take(3000)}") }
        appendLine("종목 후보: ${input.stocks.joinToString { "${it.code}=${it.name}" }.ifEmpty { "(없음)" }}")
        appendLine("섹터 후보: ${input.sectors.joinToString { "${it.code}=${it.name}" }}")
        appendLine("먼저 한국 증시나 상장사에 실질적 영향이 있는 기사인지 marketRelevant로 판정하라. 단순 생활·사건·연예·스포츠 등 증시와 무관하면 false다.")
        appendLine("marketRelevant=true이면 후보 각각의 실제 관련 여부를 판정하고, 후보에 없어도 이 뉴스의 실질적 영향(정책·규제·수혜 포함)을 받는 상장사가 확실하면 stocks에 6자리 종목코드로 추가하라. 코드가 불확실한 종목은 넣지 않는다.")
        appendLine("scope는 특정 기업 뉴스면 STOCK, 업종 전반이면 SECTOR, 시장 전체면 MARKET이다. marketRelevant=false이면 모든 종목 후보도 relevant=false로 기각하라.")
        appendLine("섹터마다 impact를 판정하라 — $IMPACT_CRITERIA")
    }

    fun digestPrompt(input: DigestInput): String = buildString {
        appendLine("${input.stockName}(${input.code})의 ${input.date} 데일리 브리핑을 작성하라. 투자 조언이 아니라 정보 요약이며, 종합 3줄로.")
        appendLine("종목 뉴스: ${input.stockClusters.joinToString(" | ") { "${it.title}(${it.sentiment})" }}")
        appendLine("섹터 이슈: ${input.sectorClusters.joinToString(" | ") { "${it.title}(${it.sentiment})" }}")
        appendLine("시장 이슈: ${input.marketClusters.joinToString(" | ") { it.title }}")
    }

    fun parseSummary(node: JsonNode, input: ClusterSummaryInput): ClusterSummaryOutput =
        ClusterSummaryOutput(
            summary = node.path("summary").asText(),
            marketRelevant = node.path("marketRelevant").asBoolean(false),
            scope = runCatching { NewsScope.valueOf(node.path("scope").asText()) }.getOrDefault(NewsScope.STOCK),
            stocks = node.path("stocks").mapNotNull { stock ->
                val code = stock.path("code").asText()
                if (!code.matches(STOCK_CODE)) return@mapNotNull null
                StockVerdict(
                    code = code,
                    relevant = stock.path("relevant").asBoolean(),
                    sentiment = sentimentOf(stock),
                    confidence = stock.path("confidence").asDouble(0.0),
                    reason = stock.path("reason").asText(),
                )
            },
            sectors = node.path("sectors").mapNotNull { sector ->
                val code = sector.path("sectorCode").asText()
                if (input.sectors.none { it.code == code }) return@mapNotNull null
                SectorVerdict(
                    sectorCode = code,
                    sentiment = sentimentOf(sector),
                    impact = runCatching {
                        Impact.valueOf(sector.path("impact").asText())
                    }.getOrDefault(Impact.LOW),
                    confidence = sector.path("confidence").asDouble(0.0),
                    reason = sector.path("reason").asText(),
                )
            },
        )

    fun parseDigest(node: JsonNode): DigestOutput =
        DigestOutput(
            title = node.path("title").asText(),
            summary = node.path("summary").asText(),
        )

    private fun sentimentOf(node: JsonNode): Sentiment =
        runCatching { Sentiment.valueOf(node.path("sentiment").asText()) }.getOrDefault(Sentiment.NEUTRAL)

    const val IMPACT_CRITERIA: String =
        "HIGH는 그 업종의 실적·비용·수요에 직접적이고 단기적인 영향, " +
            "MEDIUM은 영향 경로가 명확하지만 간접적이거나 중기적인 영향, " +
            "LOW는 관련성은 있으나 영향 경로가 약하거나 일반적인 업계 언급이다."

    private val STOCK_CODE = Regex("\\d{6}")
}
