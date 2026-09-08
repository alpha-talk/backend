package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.worker.llm.config.LlmProperties
import java.nio.file.Files
import kotlin.io.path.exists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class GeminiCliLlmClientTest {
    @Test
    fun `Gemini CLI는 구독 인증과 도구 봉인으로 뉴스를 판정한다`() {
        lateinit var request: CliProcessRequest
        lateinit var policy: String
        val runner = CliProcessRunner {
            request = it
            val policyPath = it.command[it.command.indexOf("--policy") + 1]
            policy = Files.readString(java.nio.file.Path.of(policyPath))
            CliProcessResult(
                exitCode = 0,
                stdout = """
                    {
                      "response": "```json\n{\"summary\":\"요약\",\"marketRelevant\":true,\"scope\":\"STOCK\",\"stocks\":[{\"code\":\"005930\",\"relevant\":true,\"sentiment\":\"POSITIVE\",\"confidence\":0.9,\"reason\":\"실적 개선\",\"relation\":\"DIRECT\",\"evidence\":\"삼성전자가 신규 공급 계약\"}],\"sectors\":[]}\n```",
                      "stats": {}
                    }
                """.trimIndent(),
                stderr = "",
            )
        }
        val client = GeminiCliLlmClient(
            props = LlmProperties(
                provider = "gemini-cli",
                geminiCli = LlmProperties.GeminiCli(
                    executable = "gemini-test",
                    model = "gemini-3.1-flash-lite",
                ),
            ),
            runner = runner,
        )

        val output = client.summarize(summaryInput())

        assertEquals(NewsScope.STOCK, output.scope)
        assertEquals(Sentiment.POSITIVE, output.stocks.single().sentiment)
        assertEquals("gemini-test", request.command.first())
        assertEquals("gemini-3.1-flash-lite", request.command[request.command.indexOf("--model") + 1])
        assertEquals("json", request.command[request.command.indexOf("--output-format") + 1])
        assertEquals("none", request.command[request.command.indexOf("--extensions") + 1])
        assertTrue(request.command.contains("--skip-trust"))
        assertEquals("", request.command[request.command.indexOf("--prompt") + 1])
        assertTrue(policy.contains("toolName = \"*\""))
        assertTrue(policy.contains("decision = \"deny\""))
        assertTrue("GEMINI_API_KEY" in request.environmentRemovals)
        assertTrue("GOOGLE_API_KEY" in request.environmentRemovals)
        assertTrue(request.input.contains("JSON Schema"))
        assertTrue(request.input.contains("삼성전자"))
        assertFalse(request.workingDirectory!!.exists())
    }

    private fun summaryInput() = ClusterSummaryInput(
        repTitle = "삼성전자가 신규 공급 계약",
        articleTitles = listOf("삼성전자 공급 계약"),
        body = null,
        stocks = listOf(StockCandidate("005930", "삼성전자")),
        sectors = emptyList(),
    )
}
