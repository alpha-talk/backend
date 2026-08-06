package com.alphatalk.worker.llm.enrich

import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.databind.JsonNode
import java.nio.file.Files
import java.nio.file.Path

internal class CodexCliLlmClient(
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
        val directory = Files.createTempDirectory("alphatalk-codex-")
        val schemaFile = directory.resolve("output-schema.json")
        val outputFile = directory.resolve("last-message.json")
        try {
            Files.writeString(schemaFile, StructuredLlmCodec.mapper.writeValueAsString(schema))
            val command = mutableListOf(props.codexCli.executable)
            command.addAll(
                listOf(
                    "exec",
                    "--sandbox",
                    "read-only",
                    "--ephemeral",
                    "--ignore-user-config",
                    "--ignore-rules",
                    "--skip-git-repo-check",
                    "--color",
                    "never",
                    "--cd",
                    directory.toString(),
                    "--output-schema",
                    schemaFile.toString(),
                    "--output-last-message",
                    outputFile.toString(),
                ),
            )
            props.codexCli.model.takeIf(String::isNotBlank)?.let {
                command.addAll(listOf("--model", it))
            }
            command.add("-")
            runner.run(
                CliProcessRequest(
                    command = command,
                    input = "$SYSTEM_PROMPT\n\n$prompt",
                    timeout = props.codexCli.timeout,
                    environmentRemovals = setOf("OPENAI_API_KEY"),
                    workingDirectory = directory,
                ),
            ).stdoutOrThrow("Codex")
            if (!Files.exists(outputFile)) {
                throw IllegalStateException("Codex CLI가 최종 응답 파일을 생성하지 않았다")
            }
            return StructuredLlmCodec.mapper.readTree(Files.readString(outputFile))
                ?: throw IllegalStateException("Codex CLI가 빈 JSON을 반환했다")
        } finally {
            deleteIfExists(outputFile)
            deleteIfExists(schemaFile)
            deleteIfExists(directory)
        }
    }

    private fun deleteIfExists(path: Path) {
        runCatching { Files.deleteIfExists(path) }
    }

    private companion object {
        const val SYSTEM_PROMPT =
            "도구를 호출하거나 파일을 읽지 말고, 제공된 뉴스만 분석해 지정된 JSON 스키마에 맞는 결과만 반환하라."
    }
}
