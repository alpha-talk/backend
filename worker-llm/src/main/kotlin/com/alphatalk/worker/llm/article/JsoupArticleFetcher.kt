package com.alphatalk.worker.llm.article

import org.jsoup.Jsoup
import org.slf4j.LoggerFactory

class JsoupArticleFetcher : ArticleFetcher {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun fetchBody(url: String): String? = runCatching {
        val document = Jsoup.connect(url)
            .userAgent("AlphaTalkLlm/0.1")
            .timeout(8_000)
            .get()
        val container = BODY_SELECTORS.asSequence()
            .map { document.select(it) }
            .firstOrNull { it.isNotEmpty() }
        val text = (container?.text() ?: document.body()?.text()).orEmpty().trim()
        text.take(MAX_BODY_LENGTH).ifBlank { null }
    }.onFailure {
        log.debug("article fetch failed: url={} reason={}", url, it.message)
    }.getOrNull()

    companion object {
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
