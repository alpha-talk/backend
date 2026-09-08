package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Files
import java.nio.file.Path

internal class GeminiCliLlmClient(
    private val props: LlmProperties,
    private val runner: CliProcessRunner = JvmCliProcessRunner(),
) : LlmClient {

    override fun summarize(input: ClusterSummaryInput): ClusterSummaryOutput {
        val output = call(StructuredLlmCodec.summaryPrompt(input), StructuredLlmCodec.summarySchema)
        return StructuredLlmCodec.parseSummary(output, input)
    }

    override fun digest(input: DigestInput): DigestOutput {
        val output = call(StructuredLlmCodec.digestPrompt(input), StructuredLlmCodec.digestSchema)
        return StructuredLlmCodec.parseDigest(output)
    }

    override fun marketDigest(input: MarketDigestInput): MarketDigestOutput {
        val output = call(
            StructuredLlmCodec.marketDigestPrompt(input.copy(research = false)),
            StructuredLlmCodec.marketDigestSchema,
        )
        return StructuredLlmCodec.parseMarketDigest(output, research = false)
    }

    private fun call(prompt: String, schema: Map<String, Any>): JsonNode {
        val directory = Files.createTempDirectory("alphatalk-gemini-")
        val policyFile = directory.resolve("deny-tools.toml")
        return try {
            Files.writeString(policyFile, DENY_TOOLS_POLICY)
            val schemaJson = StructuredLlmCodec.mapper.writeValueAsString(schema)
            val requestPrompt = "$SYSTEM_PROMPT\nJSON Schema:\n$schemaJson\n\n$prompt"
            val command = listOf(
                props.geminiCli.executable,
                "--model",
                props.geminiCli.model,
                "--output-format",
                "json",
                "--extensions",
                "none",
                "--skip-trust",
                "--policy",
                policyFile.toString(),
                "--prompt",
                "",
            )
            val raw = runner.run(
                CliProcessRequest(
                    command = command,
                    input = requestPrompt,
                    timeout = props.geminiCli.timeout,
                    environmentRemovals = setOf(
                        "GEMINI_API_KEY",
                        "GOOGLE_API_KEY",
                        "GOOGLE_APPLICATION_CREDENTIALS",
                        "GOOGLE_GENAI_USE_VERTEXAI",
                    ),
                    workingDirectory = directory,
                ),
            ).stdoutOrThrow("Gemini")
            structuredOutput(raw).also { StructuredLlmCodec.validate(it, schema) }
        } finally {
            deleteIfExists(policyFile)
            deleteIfExists(directory)
        }
    }

    private fun structuredOutput(raw: String): JsonNode {
        val root = StructuredLlmCodec.mapper.readTree(raw)
            ?: throw IllegalStateException("Gemini CLI가 빈 JSON을 반환했다")
        if (root.path("error").isObject) {
            throw IllegalStateException("Gemini CLI가 오류 결과를 반환했다")
        }
        val response = root.path("response").takeIf(JsonNode::isTextual)?.asText()
            ?.takeIf(String::isNotBlank)
            ?: throw IllegalStateException("Gemini CLI 응답에 response가 없다")
        val json = response.removePrefix("```json").removePrefix("```").removeSuffix("```").trim()
        val start = json.indexOf('{')
        val end = json.lastIndexOf('}')
        check(start >= 0 && end > start) {
            "Gemini CLI 응답에서 JSON 객체를 찾을 수 없다"
        }
        return StructuredLlmCodec.mapper.readTree(json.substring(start, end + 1))
    }

    private fun deleteIfExists(path: Path) {
        runCatching { Files.deleteIfExists(path) }
    }

    private companion object {
        const val SYSTEM_PROMPT =
            "외부 도구나 파일을 사용하지 말고, 제공된 뉴스만 분석하라. " +
                "응답은 주어진 JSON Schema와 정확히 일치하는 JSON 객체 하나만 반환하고 Markdown 코드 펜스를 쓰지 마라."
        const val DENY_TOOLS_POLICY =
            "[[rule]]\ntoolName = \"*\"\ndecision = \"deny\"\npriority = 999\ninteractive = false\n"
    }
}
