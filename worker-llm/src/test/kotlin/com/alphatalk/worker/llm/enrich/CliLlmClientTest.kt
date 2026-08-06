package com.alphatalk.worker.llm.enrich

import com.alphatalk.contracts.envelope.NewsScope
import com.alphatalk.contracts.envelope.Sentiment
import com.alphatalk.worker.llm.config.LlmProperties
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class CliLlmClientTest {

    @Test
    fun `Claude CLI는 구독 인증과 구조화 출력으로 뉴스를 판정한다`() {
        lateinit var request: CliProcessRequest
        val runner = CliProcessRunner {
            request = it
            CliProcessResult(
                exitCode = 0,
                stdout = """
                    {
                      "type": "result",
                      "is_error": false,
                      "structured_output": {
                        "summary": "삼성전자가 반도체 공급 계약을 체결했다.",
                        "marketRelevant": true,
                        "scope": "STOCK",
                        "stocks": [
                          {
                            "code": "005930",
                            "relevant": true,
                            "sentiment": "POSITIVE",
                            "confidence": 0.91,
                            "reason": "공급 계약"
                          }
                        ],
                        "sectors": []
                      }
                    }
                """.trimIndent(),
                stderr = "",
            )
        }
        val client = ClaudeCliLlmClient(
            props = LlmProperties(
                provider = "claude-cli",
                claudeCli = LlmProperties.ClaudeCli(
                    executable = "claude-test",
                    timeout = Duration.ofSeconds(3),
                ),
            ),
            runner = runner,
        )

        val output = client.summarize(summaryInput())

        assertTrue(output.marketRelevant)
        assertEquals(NewsScope.STOCK, output.scope)
        assertEquals(Sentiment.POSITIVE, output.stocks.single().sentiment)
        assertEquals("claude-test", request.command.first())
        assertTrue(request.command.containsAll(listOf("--json-schema", "--safe-mode", "--no-session-persistence")))
        assertFalse(request.command.contains("--bare"))
        assertEquals("sonnet", request.command[request.command.indexOf("--model") + 1])
        assertEquals("", request.command[request.command.indexOf("--tools") + 1])
        assertTrue("ANTHROPIC_API_KEY" in request.environmentRemovals)
        assertTrue(request.input.contains("삼성전자"))
    }

    @Test
    fun `Claude CLI result 문자열의 JSON도 다이제스트로 파싱한다`() {
        val runner = CliProcessRunner {
            CliProcessResult(
                exitCode = 0,
                stdout = """
                    {
                      "type": "result",
                      "is_error": false,
                      "result": "{\"title\":\"삼성전자 데일리\",\"summary\":\"주요 계약 소식\"}"
                    }
                """.trimIndent(),
                stderr = "",
            )
        }
        val client = ClaudeCliLlmClient(
            props = LlmProperties(
                provider = "claude-cli",
                claudeCli = LlmProperties.ClaudeCli(timeout = Duration.ofSeconds(3)),
            ),
            runner = runner,
        )

        val output = client.digest(digestInput())

        assertEquals("삼성전자 데일리", output.title)
        assertEquals("주요 계약 소식", output.summary)
    }

    @Test
    fun `Codex CLI는 격리된 작업공간과 출력 스키마로 뉴스를 판정한다`() {
        lateinit var request: CliProcessRequest
        lateinit var schemaPath: Path
        lateinit var outputPath: Path
        val runner = CliProcessRunner {
            request = it
            schemaPath = Path.of(it.command[it.command.indexOf("--output-schema") + 1])
            outputPath = Path.of(it.command[it.command.indexOf("--output-last-message") + 1])
            assertTrue(Files.readString(schemaPath).contains("\"marketRelevant\""))
            Files.writeString(
                outputPath,
                """
                    {
                      "summary": "삼성전자가 반도체 공급 계약을 체결했다.",
                      "marketRelevant": true,
                      "scope": "STOCK",
                      "stocks": [
                        {
                          "code": "005930",
                          "relevant": true,
                          "sentiment": "POSITIVE",
                          "confidence": 0.91,
                          "reason": "공급 계약"
                        }
                      ],
                      "sectors": []
                    }
                """.trimIndent(),
            )
            CliProcessResult(exitCode = 0, stdout = "", stderr = "")
        }
        val client = CodexCliLlmClient(
            props = LlmProperties(
                provider = "codex-cli",
                codexCli = LlmProperties.CodexCli(
                    executable = "codex-test",
                    model = "gpt-test",
                    timeout = Duration.ofSeconds(3),
                ),
            ),
            runner = runner,
        )

        val output = client.summarize(summaryInput())

        assertEquals(Sentiment.POSITIVE, output.stocks.single().sentiment)
        assertEquals("codex-test", request.command.first())
        assertTrue(request.command.containsAll(listOf("exec", "--ephemeral", "--ignore-user-config", "--ignore-rules")))
        assertEquals("read-only", request.command[request.command.indexOf("--sandbox") + 1])
        assertEquals("gpt-test", request.command[request.command.indexOf("--model") + 1])
        assertTrue("OPENAI_API_KEY" in request.environmentRemovals)
        assertEquals(request.workingDirectory.toString(), request.command[request.command.indexOf("--cd") + 1])
        assertFalse(Files.exists(schemaPath))
        assertFalse(Files.exists(outputPath))
    }

    @Test
    fun `CLI 실패 메시지는 진단을 위해 stderr를 함께 싣는다`() {
        val runner = CliProcessRunner {
            CliProcessResult(exitCode = 1, stdout = "", stderr = "Usage limit reached. Resets at 6pm.")
        }
        val client = ClaudeCliLlmClient(LlmProperties(), runner)

        val e = assertFailsWith<IllegalStateException> { client.summarize(summaryInput()) }

        assertTrue("Usage limit reached" in e.message.orEmpty())
        assertTrue("종료 코드 1" in e.message.orEmpty())
    }

    @Test
    fun `긴 stderr는 끝부분만 남긴다`() {
        val runner = CliProcessRunner {
            CliProcessResult(exitCode = 1, stdout = "", stderr = "x".repeat(900) + "REAL-CAUSE")
        }
        val client = ClaudeCliLlmClient(LlmProperties(), runner)

        val message = assertFailsWith<IllegalStateException> { client.summarize(summaryInput()) }.message.orEmpty()

        assertTrue("REAL-CAUSE" in message)
        assertTrue(message.length < 900)
    }

    private fun summaryInput() = ClusterSummaryInput(
        repTitle = "삼성전자 반도체 공급 계약",
        articleTitles = listOf("삼성전자 공급 확대"),
        body = "삼성전자가 신규 공급 계약을 체결했다.",
        stocks = listOf(StockCandidate("005930", "삼성전자")),
        sectors = listOf(SectorCandidate("33", "반도체")),
    )

    private fun digestInput() = DigestInput(
        code = "005930",
        stockName = "삼성전자",
        date = "2026-07-24",
        stockClusters = emptyList(),
        sectorClusters = emptyList(),
        marketClusters = emptyList(),
    )
}
