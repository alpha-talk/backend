package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.databind.JsonNode
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.http.MediaType
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient

class AnthropicLlmClient(
    private val props: LlmProperties,
    private val meters: MeterRegistry,
    private val rest: RestClient = RestClient.builder()
        .baseUrl(props.anthropic.baseUrl)
        .requestFactory(
            SimpleClientHttpRequestFactory().apply {
                setConnectTimeout(props.anthropic.connectTimeout)
                setReadTimeout(props.anthropic.readTimeout)
            },
        )
        .build(),
) : LlmClient {

    private val mapper = StructuredLlmCodec.mapper

    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val toolInput = callTool(
            model = props.models.summary,
            tool = SUMMARIZE_TOOL,
            prompt = StructuredLlmCodec.summaryPrompt(input),
        )
        return StructuredLlmCodec.parseSummary(toolInput, input)
    }

    override fun digest(input: DigestInput): DigestOutput {
        val toolInput = callTool(
            model = props.models.digest,
            tool = DIGEST_TOOL,
            prompt = StructuredLlmCodec.digestPrompt(input),
        )
        return StructuredLlmCodec.parseDigest(toolInput)
    }

    override fun marketDigest(input: MarketDigestInput): MarketDigestOutput {
        val toolInput = callTool(
            model = props.models.digest,
            tool = MARKET_DIGEST_TOOL,
            prompt = StructuredLlmCodec.marketDigestPrompt(input.copy(research = false)),
        )
        return StructuredLlmCodec.parseMarketDigest(toolInput, research = false)
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

    companion object {
        private val SUMMARIZE_TOOL = mapOf(
            "name" to "submit_news_analysis",
            "description" to "뉴스 클러스터 요약·감성 판정 결과 제출",
            "input_schema" to StructuredLlmCodec.summarySchema,
        )

        private val DIGEST_TOOL = mapOf(
            "name" to "submit_daily_digest",
            "description" to "종목 데일리 브리핑 제출",
            "input_schema" to StructuredLlmCodec.digestSchema,
        )

        private val MARKET_DIGEST_TOOL = mapOf(
            "name" to "submit_market_digest",
            "description" to "시장 데일리 브리핑 제출",
            "input_schema" to StructuredLlmCodec.marketDigestSchema,
        )
    }
}
