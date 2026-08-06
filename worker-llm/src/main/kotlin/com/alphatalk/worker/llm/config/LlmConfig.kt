package com.alphatalk.worker.llm.config

import com.alphatalk.worker.llm.cluster.EmbeddingClient
import com.alphatalk.worker.llm.cluster.FakeEmbeddingClient
import com.alphatalk.worker.llm.cluster.RestEmbeddingClient
import com.alphatalk.worker.llm.enrich.AnthropicLlmClient
import com.alphatalk.worker.llm.enrich.ClaudeCliLlmClient
import com.alphatalk.worker.llm.enrich.CodexCliLlmClient
import com.alphatalk.worker.llm.enrich.FakeLlmClient
import com.alphatalk.worker.llm.enrich.LlmClient
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Duration

@Configuration
class LlmConfig {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    fun embeddingClient(props: LlmProperties): EmbeddingClient = when {
        props.embedding.provider == "rest" -> {
            val e = props.embedding
            check(e.baseUrl.isNotBlank() && e.apiKey.isNotBlank() && e.model.isNotBlank()) {
                "embedding provider=rest에는 base-url·api-key·model이 모두 필요하다"
            }
            RestEmbeddingClient(e)
        }
        props.allowFake -> {
            log.info("using fake embedding client (provider={}, allow-fake)", props.embedding.provider)
            FakeEmbeddingClient(props.embedding.dimension)
        }
        else -> throw IllegalStateException(
            "임베딩 미구성 — provider=rest(base-url·api-key·model) 설정 필수. 로컬·테스트는 alphatalk.llm.allow-fake=true",
        )
    }

    @Bean
    fun llmClient(props: LlmProperties, meters: MeterRegistry): LlmClient = when (props.provider.trim().lowercase()) {
        "anthropic" -> {
            check(props.anthropic.apiKey.isNotBlank()) {
                "LLM provider=anthropic에는 ANTHROPIC_API_KEY가 필요하다"
            }
            val boundedTimeouts = listOf(props.anthropic.connectTimeout, props.anthropic.readTimeout)
                .all { !it.isZero && !it.isNegative }
            check(boundedTimeouts) {
                "anthropic connect/read timeout은 양수여야 한다 — 0은 무한 대기라 claim-idle을 넘길 수 있다"
            }
            AnthropicLlmClient(props, meters)
        }
        "claude-cli" -> {
            check(props.claudeCli.executable.isNotBlank()) {
                "LLM provider=claude-cli에는 실행 파일 경로가 필요하다"
            }
            validateCliConsumer(props, props.claudeCli.timeout, researchCapable = true)
            log.info("using Claude CLI LLM client (subscription auth)")
            ClaudeCliLlmClient(props)
        }
        "codex-cli" -> {
            check(props.codexCli.executable.isNotBlank()) {
                "LLM provider=codex-cli에는 실행 파일 경로가 필요하다"
            }
            validateCliConsumer(props, props.codexCli.timeout)
            log.info("using Codex CLI LLM client (subscription auth)")
            CodexCliLlmClient(props)
        }
        "fake" -> {
            check(props.allowFake) {
                "LLM provider=fake는 alphatalk.llm.allow-fake=true일 때만 허용된다"
            }
            log.info("using rule-based fake LLM client")
            FakeLlmClient()
        }
        else -> throw IllegalStateException(
            "지원하지 않는 LLM provider=${props.provider}. anthropic|claude-cli|codex-cli|fake 중 하나여야 한다",
        )
    }

    private fun validateCliConsumer(props: LlmProperties, timeout: Duration, researchCapable: Boolean = false) {
        check(props.consumerBatch == 1) {
            "CLI LLM provider는 PEL 선점 충돌 방지를 위해 consumer-batch=1이어야 한다"
        }
        check(!timeout.isZero && !timeout.isNegative && timeout < props.claimIdle) {
            "CLI LLM timeout은 양수이고 claim-idle(${props.claimIdle})보다 짧아야 한다"
        }
        if (researchCapable && props.market.researchEnabled) {
            val batchWait = timeout.multipliedBy((props.consumerBatch - 1).toLong())
            val worstCase = batchWait.plus(props.market.researchTimeout).plus(timeout)
            check(!props.market.researchTimeout.isZero && !props.market.researchTimeout.isNegative && worstCase < props.claimIdle) {
                "시장 리서치 데드라인은 배치 대기·무리서치 재호출 포함 claim-idle(${props.claimIdle})보다 짧아야 한다 — " +
                    "(consumer-batch-1)×timeout + research-timeout + timeout = $worstCase"
            }
        }
    }
}
