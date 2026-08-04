package com.alphatalk.worker.llm.enrich

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.TimeUnit

internal data class CliProcessRequest(
    val command: List<String>,
    val input: String,
    val timeout: Duration,
    val environmentRemovals: Set<String> = emptySet(),
    val workingDirectory: Path? = null,
)

internal data class CliProcessResult(
    val exitCode: Int,
    val stdout: String,
    val stderr: String,
)

internal fun interface CliProcessRunner {
    fun run(request: CliProcessRequest): CliProcessResult
}

internal class JvmCliProcessRunner : CliProcessRunner {
    override fun run(request: CliProcessRequest): CliProcessResult {
        require(!request.timeout.isZero && !request.timeout.isNegative) {
            "CLI timeout은 양수여야 한다"
        }
        val stdoutFile = Files.createTempFile("alphatalk-llm-", ".stdout")
        val stderrFile = Files.createTempFile("alphatalk-llm-", ".stderr")
        try {
            val builder = ProcessBuilder(request.command)
                .redirectOutput(stdoutFile.toFile())
                .redirectError(stderrFile.toFile())
            request.workingDirectory?.let { builder.directory(it.toFile()) }
            request.environmentRemovals.forEach(builder.environment()::remove)
            val process = builder.start()
            try {
                process.outputStream.bufferedWriter(StandardCharsets.UTF_8).use { writer ->
                    writer.write(request.input)
                }
                if (!process.waitFor(request.timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                    process.destroyForcibly()
                    process.waitFor()
                    throw IllegalStateException("LLM CLI 응답 시간이 ${request.timeout}을 초과했다")
                }
                return CliProcessResult(
                    exitCode = process.exitValue(),
                    stdout = Files.readString(stdoutFile),
                    stderr = Files.readString(stderrFile),
                )
            } catch (interrupted: InterruptedException) {
                process.destroyForcibly()
                Thread.currentThread().interrupt()
                throw IllegalStateException("LLM CLI 실행이 중단됐다", interrupted)
            } catch (failure: Exception) {
                if (process.isAlive) process.destroyForcibly()
                throw failure
            }
        } finally {
            Files.deleteIfExists(stdoutFile)
            Files.deleteIfExists(stderrFile)
        }
    }
}

internal fun CliProcessResult.stdoutOrThrow(provider: String): String {
    if (exitCode == 0) return stdout
    throw IllegalStateException(
        "$provider CLI가 종료 코드 $exitCode 로 실패했다. CLI 로그인 상태와 provider 설정을 확인하라. " +
            "stderr=${diagnosticTail(stderr)} stdout=${diagnosticTail(stdout)}",
    )
}

private const val DIAGNOSTIC_TAIL_CHARS = 500

private fun diagnosticTail(text: String): String {
    val trimmed = text.trim()
    if (trimmed.isEmpty()) return "(비어 있음)"
    return if (trimmed.length <= DIAGNOSTIC_TAIL_CHARS) {
        trimmed
    } else {
        "…" + trimmed.takeLast(DIAGNOSTIC_TAIL_CHARS)
    }
}
