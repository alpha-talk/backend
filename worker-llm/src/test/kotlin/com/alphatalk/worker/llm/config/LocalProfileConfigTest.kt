package com.alphatalk.worker.llm.config

import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.env.YamlPropertySourceLoader
import org.springframework.core.io.ClassPathResource
import org.springframework.mock.env.MockEnvironment
import kotlin.test.assertEquals

class LocalProfileConfigTest {
    @Test
    fun `local 프로파일은 Claude CLI와 Ollama BGE-M3를 기본 사용하고 명시 설정을 허용한다`() {
        val defaults = localProperties()

        assertEquals("claude-cli", defaults.provider)
        assertEquals("sonnet", defaults.claudeCli.model)
        assertEquals(1, defaults.consumerBatch)
        assertEquals("rest", defaults.embedding.provider)
        assertEquals("http://localhost:11434", defaults.embedding.baseUrl)
        assertEquals("ollama", defaults.embedding.apiKey)
        assertEquals("bge-m3", defaults.embedding.model)
        assertEquals(1024, defaults.embedding.dimension)

        val overrides = localEnvironment().withProperty("LLM_PROVIDER", "codex-cli")
            .withProperty("alphatalk.llm.claude-cli.model", "opus")
            .withProperty("EMBEDDING_PROVIDER", "fake")
            .let(::bind)

        assertEquals("codex-cli", overrides.provider)
        assertEquals("opus", overrides.claudeCli.model)
        assertEquals("fake", overrides.embedding.provider)
    }

    private fun localProperties(): LlmProperties = bind(localEnvironment())

    private fun bind(environment: MockEnvironment): LlmProperties =
        Binder.get(environment)
            .bind("alphatalk.llm", Bindable.of(LlmProperties::class.java))
            .get()

    private fun localEnvironment(): MockEnvironment {
        val environment = MockEnvironment()
        val loader = YamlPropertySourceLoader()
        loader.load("application", ClassPathResource("application.yml"))
            .forEach(environment.propertySources::addLast)
        loader.load("application-local", ClassPathResource("application-local.yml"))
            .forEach(environment.propertySources::addFirst)
        return environment
    }
}
