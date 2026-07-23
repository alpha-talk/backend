package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.http.MediaType
import org.springframework.web.client.RestClient

class AnthropicLlmClient(
    private val props: LlmProperties,
    private val meters: MeterRegistry,
    private val rest: RestClient = RestClient.builder().baseUrl(props.anthropic.baseUrl).build(),
) : LlmClient {

    private val mapper = jacksonObjectMapper()

    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val toolInput = callTool(
            model = props.models.summary,
            tool = SUMMARIZE_TOOL,
            prompt = summarizePrompt(input),
        )
        return parseSummary(toolInput, input)
    }

    override fun digest(input: DigestInput): DigestOutput {
        val toolInput = callTool(
            model = props.models.digest,
            tool = DIGEST_TOOL,
            prompt = digestPrompt(input),
        )
        return DigestOutput(
            title = toolInput.path("title").asText(),
            summary = toolInput.path("summary").asText(),
        )
    }

    private fun callTool(model: String, tool: Map<String, Any>, prompt: String): JsonNode {
        val body = mapOf(
            "model" to model,
            "max_tokens" to props.anthropic.maxTokens,
            "tools" to listOf(tool),
            "tool_choice" to mapOf("type" to "tool", "name" to tool["name"]),
            "messages" to listOf(mapOf("role" to "user", "content" to prompt)),
        )
        val response = rest.post()
            .uri("/v1/messages")
            .header("x-api-key", props.anthropic.apiKey)
            .header("anthropic-version", props.anthropic.version)
            .contentType(MediaType.APPLICATION_JSON)
            .body(mapper.writeValueAsString(body))
            .retrieve()
            .body(String::class.java)
            ?: throw IllegalStateException("empty anthropic response")
        val root = mapper.readTree(response)
        root.path("usage").let {
            meters.counter("llm.tokens", "model", model, "kind", "input")
                .increment(it.path("input_tokens").asDouble())
            meters.counter("llm.tokens", "model", model, "kind", "output")
                .increment(it.path("output_tokens").asDouble())
        }
        return root.path("content").firstOrNull { it.path("type").asText() == "tool_use" }
            ?.path("input")
            ?: throw IllegalStateException("no tool_use block in anthropic response")
    }

    private fun parseSummary(node: JsonNode, input: ClusterSummaryInput): ClusterSummaryOutput =
        ClusterSummaryOutput(
            summary = node.path("summary").asText(),
            marketRelevant = node.path("marketRelevant").asBoolean(false),
            scope = runCatching { NewsScope.valueOf(node.path("scope").asText()) }.getOrDefault(NewsScope.STOCK),
            stocks = node.path("stocks").mapNotNull { s ->
                val code = s.path("code").asText()
                if (!code.matches(STOCK_CODE)) return@mapNotNull null
                StockVerdict(
                    code = code,
                    relevant = s.path("relevant").asBoolean(),
                    sentiment = sentimentOf(s),
                    confidence = s.path("confidence").asDouble(0.0),
                    reason = s.path("reason").asText(),
                )
            },
            sectors = node.path("sectors").mapNotNull { s ->
                val code = s.path("sectorCode").asText()
                if (input.sectors.none { it.code == code }) return@mapNotNull null
                SectorVerdict(
                    sectorCode = code,
                    sentiment = sentimentOf(s),
                    impact = runCatching { Impact.valueOf(s.path("impact").asText()) }.getOrDefault(Impact.LOW),
                    confidence = s.path("confidence").asDouble(0.0),
                    reason = s.path("reason").asText(),
                )
            },
        )

    private fun sentimentOf(node: JsonNode): Sentiment =
        runCatching { Sentiment.valueOf(node.path("sentiment").asText()) }.getOrDefault(Sentiment.NEUTRAL)

    private fun summarizePrompt(input: ClusterSummaryInput): String = buildString {
        appendLine("다음 뉴스를 3줄로 요약하고 영향받는 종목·섹터를 판정하라. 투자 조언이 아니라 정보 요약이다.")
        appendLine("대표 제목: ${input.repTitle}")
        appendLine("클러스터 기사 제목들: ${input.articleTitles.joinToString(" | ")}")
        input.body?.let { appendLine("본문: ${it.take(3000)}") }
        appendLine("종목 후보: ${input.stocks.joinToString { "${it.code}=${it.name}" }.ifEmpty { "(없음)" }}")
        appendLine("섹터 후보: ${input.sectors.joinToString { "${it.code}=${it.name}" }}")
        appendLine("먼저 한국 증시나 상장사에 실질적 영향이 있는 기사인지 marketRelevant로 판정하라. 단순 생활·사건·연예·스포츠 등 증시와 무관하면 false다.")
        appendLine("marketRelevant=true이면 후보 각각의 실제 관련 여부를 판정하고, 후보에 없어도 이 뉴스의 실질적 영향(정책·규제·수혜 포함)을 받는 상장사가 확실하면 stocks에 6자리 종목코드로 추가하라. 코드가 불확실한 종목은 넣지 않는다.")
        appendLine("scope는 특정 기업 뉴스면 STOCK, 업종 전반이면 SECTOR, 시장 전체면 MARKET이다. marketRelevant=false이면 모든 종목 후보도 relevant=false로 기각하라.")
    }

    private fun digestPrompt(input: DigestInput): String = buildString {
        appendLine("${input.stockName}(${input.code})의 ${input.date} 데일리 브리핑을 작성하라. 투자 조언이 아니라 정보 요약이며, 종합 3줄로.")
        appendLine("종목 뉴스: ${input.stockClusters.joinToString(" | ") { "${it.title}(${it.sentiment})" }}")
        appendLine("섹터 이슈: ${input.sectorClusters.joinToString(" | ") { "${it.title}(${it.sentiment})" }}")
        appendLine("시장 이슈: ${input.marketClusters.joinToString(" | ") { it.title }}")
    }

    companion object {
        private val STOCK_CODE = Regex("\\d{6}")

        private val SUMMARIZE_TOOL = mapOf(
            "name" to "submit_news_analysis",
            "description" to "뉴스 클러스터 요약·감성 판정 결과 제출",
            "input_schema" to mapOf(
                "type" to "object",
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
                            "properties" to mapOf(
                                "code" to mapOf("type" to "string"),
                                "relevant" to mapOf("type" to "boolean"),
                                "sentiment" to mapOf("type" to "string", "enum" to listOf("POSITIVE", "NEGATIVE", "NEUTRAL")),
                                "confidence" to mapOf("type" to "number"),
                                "reason" to mapOf("type" to "string"),
                            ),
                            "required" to listOf("code", "relevant", "sentiment", "confidence"),
                        ),
                    ),
                    "sectors" to mapOf(
                        "type" to "array",
                        "items" to mapOf(
                            "type" to "object",
                            "properties" to mapOf(
                                "sectorCode" to mapOf("type" to "string"),
                                "sentiment" to mapOf("type" to "string", "enum" to listOf("POSITIVE", "NEGATIVE", "NEUTRAL")),
                                "impact" to mapOf("type" to "string", "enum" to listOf("HIGH", "MEDIUM", "LOW")),
                                "confidence" to mapOf("type" to "number"),
                                "reason" to mapOf("type" to "string"),
                            ),
                            "required" to listOf("sectorCode", "sentiment", "impact", "confidence"),
                        ),
                    ),
                ),
                "required" to listOf("summary", "marketRelevant", "scope", "stocks"),
            ),
        )

        private val DIGEST_TOOL = mapOf(
            "name" to "submit_daily_digest",
            "description" to "종목 데일리 브리핑 제출",
            "input_schema" to mapOf(
                "type" to "object",
                "properties" to mapOf(
                    "title" to mapOf("type" to "string"),
                    "summary" to mapOf("type" to "string", "description" to "종합 3줄"),
                ),
                "required" to listOf("title", "summary"),
            ),
        )
    }
}
