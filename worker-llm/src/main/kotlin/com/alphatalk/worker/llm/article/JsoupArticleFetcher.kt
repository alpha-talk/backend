package com.alphatalk.worker.llm.article

import org.jsoup.Jsoup
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.net.URI
import java.time.Clock
import java.time.Duration
import java.time.Instant

@Component
class JsoupArticleFetcher(
    private val policy: ArticleUrlPolicy,
    private val robots: RobotsPolicy,
    private val gate: ArticleRequestGate,
    private val clock: Clock = Clock.systemUTC(),
) : ArticleFetcher {
    private val log = LoggerFactory.getLogger(javaClass)

    init {
        if (!policy.enabled) {
            log.info("article body fetch 비활성 — allowed-host-suffixes 미설정(제목·발췌만 사용)")
        }
    }

    override fun fetchBody(url: String): String? {
        if (!policy.enabled) return null
        val deadline = clock.instant().plus(FETCH_DEADLINE)
        return runCatching fetch@{
            var current: URI = policy.permit(url) ?: return@fetch null
            var redirects = 0
            while (redirects <= MAX_REDIRECTS) {
                if (expired(deadline)) return@fetch null
                if (!robots.allowed(current)) return@fetch null
                if (expired(deadline)) return@fetch null
                gate.await(current)
                if (expired(deadline)) return@fetch null
                val response = Jsoup.connect(current.toString())
                    .userAgent(USER_AGENT)
                    .timeout(FETCH_TIMEOUT.toMillis().toInt())
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

    private fun expired(deadline: Instant): Boolean {
        if (clock.instant() < deadline) return false
        log.debug("article fetch deadline exceeded")
        return true
    }

    companion object {
        const val USER_AGENT = "AlphaTalkLlm/0.1"
        val FETCH_TIMEOUT: Duration = Duration.ofSeconds(8)
        val FETCH_DEADLINE: Duration = Duration.ofSeconds(20)
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
