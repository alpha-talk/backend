package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.databind.JsonNode

internal class ClaudeCliLlmClient(
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

    private fun call(prompt: String, schema: Map<String, Any>): JsonNode {
        val schemaJson = StructuredLlmCodec.mapper.writeValueAsString(schema)
        val command = mutableListOf(
            props.claudeCli.executable,
            "-p",
            "--output-format",
            "json",
            "--json-schema",
            schemaJson,
            "--tools",
            "",
            "--safe-mode",
            "--disable-slash-commands",
            "--no-session-persistence",
            "--max-turns",
            "1",
            "--system-prompt",
            SYSTEM_PROMPT,
        )
        props.claudeCli.model.takeIf(String::isNotBlank)?.let {
            command.addAll(listOf("--model", it))
        }
        val raw = runner.run(
            CliProcessRequest(
                command = command,
                input = prompt,
                timeout = props.claudeCli.timeout,
                environmentRemovals = setOf("ANTHROPIC_API_KEY"),
            ),
        ).stdoutOrThrow("Claude")
        val root = StructuredLlmCodec.mapper.readTree(raw)
            ?: throw IllegalStateException("Claude CLI가 빈 JSON을 반환했다")
        if (root.path("is_error").asBoolean(false)) {
            throw IllegalStateException("Claude CLI가 오류 결과를 반환했다")
        }
        return structuredOutput(root)
    }

    private fun structuredOutput(root: JsonNode): JsonNode {
        val output = when {
            root.hasNonNull("structured_output") -> root.path("structured_output")
            root.hasNonNull("structuredOutput") -> root.path("structuredOutput")
            root.path("result").isTextual -> StructuredLlmCodec.mapper.readTree(root.path("result").asText())
            root.path("type").asText() != "result" -> root
            else -> null
        }
        if (output?.isObject != true) {
            throw IllegalStateException("Claude CLI 구조화 출력을 읽을 수 없다")
        }
        return output
    }

    private companion object {
        const val SYSTEM_PROMPT =
            "외부 도구나 파일을 사용하지 말고, 제공된 뉴스만 분석해 지정된 JSON 스키마에 맞는 결과만 반환하라."
    }
}
