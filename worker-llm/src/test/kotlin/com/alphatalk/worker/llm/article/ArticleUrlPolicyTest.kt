package com.alphatalk.worker.llm.article

import com.alphatalk.worker.llm.config.LlmProperties
import org.junit.jupiter.api.Test
import java.net.URI
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class ArticleUrlPolicyTest {
    private fun policyOf(vararg allowedHostSuffixes: String) = ArticleUrlPolicy(
        LlmProperties(article = LlmProperties.Article(allowedHostSuffixes = allowedHostSuffixes.toList())),
    )

    @Test
    fun `allowlist가 비어 있으면 본문 조회를 끈다`() {
        val policy = policyOf()
        assertFalse(policy.enabled)
        assertNull(policy.permit("https://8.8.8.8/news"))
    }

    @Test
    fun `공인 IP allowlist와 일치하는 URL만 허용한다`() {
        val policy = policyOf("8.8.8.8")
        assertTrue(policy.enabled)
        assertNotNull(policy.permit("https://8.8.8.8/news"))
        assertNull(policy.permit("https://1.1.1.1/news"))
        assertNull(policy.permit("http://127.0.0.1/admin"))
    }

    @Test
    fun `redirect도 같은 allowlist를 다시 검사한다`() {
        val policy = policyOf("8.8.8.8")
        assertNotNull(policy.permitRedirect(URI("https://8.8.8.8/next")))
        assertNull(policy.permitRedirect(URI("https://1.1.1.1/next")))
    }
}
