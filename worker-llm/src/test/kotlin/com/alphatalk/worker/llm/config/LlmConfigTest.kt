package com.alphatalk.worker.llm.config

import com.alphatalk.worker.llm.enrich.AnthropicLlmClient
import com.alphatalk.worker.llm.enrich.ClaudeCliLlmClient
import com.alphatalk.worker.llm.enrich.CodexCliLlmClient
import com.alphatalk.worker.llm.enrich.FakeLlmClient
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

class LlmConfigTest {
    private val config = LlmConfig()
    private val meters = SimpleMeterRegistry()

    @Test
    fun `provider에 따라 LLM 구현체를 선택한다`() {
        assertIs<ClaudeCliLlmClient>(
            config.llmClient(LlmProperties(provider = "claude-cli", consumerBatch = 1), meters),
        )
        assertIs<CodexCliLlmClient>(
            config.llmClient(LlmProperties(provider = "codex-cli", consumerBatch = 1), meters),
        )
        assertIs<AnthropicLlmClient>(
            config.llmClient(
                LlmProperties(
                    provider = "anthropic",
                    anthropic = LlmProperties.Anthropic(apiKey = "test-key"),
                ),
                meters,
            ),
        )
        assertIs<FakeLlmClient>(
            config.llmClient(LlmProperties(provider = "fake", allowFake = true), meters),
        )
    }

    @Test
    fun `fake provider는 명시적 허용 없이는 시작하지 않는다`() {
        assertFailsWith<IllegalStateException> {
            config.llmClient(LlmProperties(provider = "fake"), meters)
        }
    }

    @Test
    fun `anthropic provider는 API 키 없이 시작하지 않는다`() {
        assertFailsWith<IllegalStateException> {
            config.llmClient(LlmProperties(provider = "anthropic"), meters)
        }
    }

    @Test
    fun `지원하지 않는 provider와 안전하지 않은 CLI 소비 설정은 시작하지 않는다`() {
        assertFailsWith<IllegalStateException> {
            config.llmClient(LlmProperties(provider = "unknown"), meters)
        }
        assertFailsWith<IllegalStateException> {
            config.llmClient(LlmProperties(provider = "claude-cli", consumerBatch = 8), meters)
        }
        assertFailsWith<IllegalStateException> {
            config.llmClient(
                LlmProperties(
                    provider = "codex-cli",
                    consumerBatch = 1,
                    codexCli = LlmProperties.CodexCli(timeout = Duration.ofMinutes(5)),
                ),
                meters,
            )
        }
    }
}
