package com.alphatalk.worker.ingest.source

import com.alphatalk.worker.ingest.normalize.ArticleNormalizer
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.net.URI
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.OffsetDateTime

class NaverSearchNewsSource(
    private val clientId: String,
    private val clientSecret: String,
    private val queries: List<StockQuery>,
    private val display: Int,
    private val open: (String, Map<String, String>) -> InputStream = ::openHttp,
) : NewsSource {
    data class StockQuery(val code: String, val query: String)

    private val log = LoggerFactory.getLogger(javaClass)
    private val mapper = jacksonObjectMapper()

    override val name: String = "naver"

    override fun fetchLatest(): List<FetchedArticle> {
        val articles = queries.flatMap { query ->
            runCatching { search(query) }.getOrElse {
                log.warn("naver search failed: code={} reason={}", query.code, it.message)
                emptyList()
            }
        }
        return articles
            .groupBy { ArticleNormalizer.normalizeUrl(it.url) }
            .values
            .map { group -> group.first().copy(codes = group.flatMap { it.codes }.distinct().sorted()) }
    }

    private fun search(query: StockQuery): List<FetchedArticle> {
        val encoded = URLEncoder.encode(query.query, StandardCharsets.UTF_8)
        val url = "https://openapi.naver.com/v1/search/news.json?query=$encoded&display=$display&sort=date"
        val headers = mapOf(
            "X-Naver-Client-Id" to clientId,
            "X-Naver-Client-Secret" to clientSecret,
        )
        val root = open(url, headers).use { mapper.readTree(it) }
        return root.path("items").mapNotNull { item ->
            val title = clean(item.path("title").asText())
            val link = item.path("originallink").asText().ifBlank { item.path("link").asText() }
            if (title.isBlank() || link.isBlank()) return@mapNotNull null
            FetchedArticle(
                title = title,
                url = link,
                excerpt = clean(item.path("description").asText()).ifBlank { null },
                publishedAt = parsePubDate(item.path("pubDate").asText()),
                codes = listOf(query.code),
            )
        }
    }

    private fun clean(value: String): String = value
        .replace(TAG, "")
        .replace("&quot;", "\"")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&apos;", "'")
        .trim()

    private fun parsePubDate(value: String): Long? = runCatching {
        OffsetDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME).toInstant().toEpochMilli()
    }.getOrNull()

    companion object {
        private val TAG = Regex("</?b>")
    }
}

private fun openHttp(url: String, headers: Map<String, String>): InputStream {
    val connection = URI(url).toURL().openConnection()
    connection.connectTimeout = 5_000
    connection.readTimeout = 10_000
    headers.forEach(connection::setRequestProperty)
    return connection.getInputStream()
}
