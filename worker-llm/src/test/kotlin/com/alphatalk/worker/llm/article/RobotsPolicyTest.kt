package com.alphatalk.worker.llm.article

import org.junit.jupiter.api.Test
import java.net.URI
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class RobotsPolicyTest {
    private val noopGate = ArticleRequestGate { }
    private val robotsTxt = """
        User-agent: *
        Disallow: /private
        Disallow: /*.pdf$
        Disallow: /search?
        Allow: /private/public

        User-agent: AlphaTalkLlm
        Disallow: /blocked
    """.trimIndent()

    private var fetchCount = 0
    private fun policy(clock: () -> Long = { 0 }) = RobotsPolicy(
        gate = noopGate,
        fetch = {
            fetchCount++
            RobotsPolicy.FetchResult.Ok(robotsTxt)
        },
        clock = clock,
    )

    @Test
    fun `허용·차단 규칙 적용`() {
        val p = policy()
        assertTrue(p.allowed(URI("https://example.com/news/1")))
        assertFalse(p.allowed(URI("https://example.com/private/1")))
        assertFalse(p.allowed(URI("https://example.com/blocked/1")))
    }

    @Test
    fun `와일드카드·앵커·쿼리 규칙`() {
        val p = policy()
        assertFalse(p.allowed(URI("https://example.com/docs/report.pdf")))
        assertFalse(p.allowed(URI("https://example.com/search?q=x")))
        assertTrue(p.allowed(URI("https://example.com/docs/report.html")))
    }

    @Test
    fun `Allow가 더 길면 차단을 이긴다`() {
        assertTrue(policy().allowed(URI("https://example.com/private/public/1")))
    }

    @Test
    fun `같은 길이 규칙이면 Allow가 Disallow를 이긴다`() {
        val tied = RobotsPolicy(
            gate = noopGate,
            fetch = {
                RobotsPolicy.FetchResult.Ok(
                    """
                    User-agent: *
                    Disallow: /same
                    Allow: /same
                    """.trimIndent(),
                )
            },
        )
        assertTrue(tied.allowed(URI("https://example.com/same")))
    }

    @Test
    fun `호스트별 robots는 캐시된다`() {
        val p = policy()
        repeat(5) { p.allowed(URI("https://example.com/news/$it")) }
        assertEquals(1, fetchCount)
    }

    @Test
    fun `robots 네트워크 조회도 요청 게이트를 거친다`() {
        var permits = 0
        val p = RobotsPolicy(
            fetch = { RobotsPolicy.FetchResult.Ok(robotsTxt) },
            gate = ArticleRequestGate { permits++ },
        )
        repeat(2) { p.allowed(URI("https://example.com/news/$it")) }
        assertEquals(1, permits)
    }

    @Test
    fun `조회 실패는 허용하되 짧게 캐시`() {
        var now = 0L
        val lenient = RobotsPolicy(
            gate = noopGate,
            fetch = { fetchCount++; RobotsPolicy.FetchResult.Unavailable },
            clock = { now },
        )
        assertTrue(lenient.allowed(URI("https://example.com/anything")))
        now = Duration.ofMinutes(6).toMillis()
        lenient.allowed(URI("https://example.com/anything"))
        assertEquals(2, fetchCount)
    }
}
