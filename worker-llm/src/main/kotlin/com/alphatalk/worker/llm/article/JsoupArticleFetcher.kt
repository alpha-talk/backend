package com.alphatalk.worker.llm.article

import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import java.net.URI

class JsoupArticleFetcher(
    private val policy: ArticleUrlPolicy,
    private val robots: RobotsPolicy = RobotsPolicy(),
    private val gate: ArticleRequestGate = ArticleRequestGate { },
) : ArticleFetcher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun fetchBody(url: String): String? {
        if (!policy.enabled) return null
        return runCatching fetch@{
            var current: URI = policy.permit(url) ?: return@fetch null
            var redirects = 0
            while (redirects <= MAX_REDIRECTS) {
                if (!robots.allowed(current)) return@fetch null
                gate.await(current)
                val response = Jsoup.connect(current.toString())
                    .userAgent(USER_AGENT)
                    .timeout(8_000)
                    .followRedirects(false)
                    .execute()
                if (response.statusCode() in 300..399) {
                    if (++redirects > MAX_REDIRECTS) return@fetch null
                    val location = response.header("Location") ?: return@fetch null
                    current = policy.permitRedirect(current.resolve(location)) ?: return@fetch null
                    continue
                }
                val document = response.parse()
                val container = BODY_SELECTORS.asSequence()
                    .map { document.select(it) }
                    .firstOrNull { it.isNotEmpty() }
                val text = (container?.text() ?: document.body()?.text()).orEmpty().trim()
                return@fetch text.take(MAX_BODY_LENGTH).ifBlank { null }
            }
            null
        }.onFailure {
            log.debug("article fetch failed: url={} reason={}", url, it.message)
        }.getOrNull()
    }

    companion object {
        const val USER_AGENT = "AlphaTalkLlm/0.1"
        private const val MAX_REDIRECTS = 3
        private val BODY_SELECTORS = listOf(
            "article",
            "#articleBody",
            "#article-body",
            ".article-body",
            ".article_body",
            "#newsct_article",
            ".news_end",
        )
        private const val MAX_BODY_LENGTH = 3_000
    }
}
