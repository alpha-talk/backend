package com.alphatalk.worker.ingest.source

import com.rometools.rome.io.SyndFeedInput
import com.rometools.rome.io.XmlReader
import java.io.ByteArrayInputStream

class RssNewsSource(
    override val id: String,
    override val name: String,
    private val feedUrl: String,
    private val client: RssFeedClient,
) : NewsSource {
    private var etag: String? = null
    private var lastModified: Long? = null

    @Synchronized
    override fun fetchLatest(): List<FetchedArticle> {
        val response = client.fetch(RssFeedRequest(feedUrl, etag, lastModified))
        if (response === RssFeedResponse.NotModified) return emptyList()
        response as RssFeedResponse.Modified
        val feed = ByteArrayInputStream(response.body).use { SyndFeedInput().build(XmlReader(it)) }
        val articles = feed.entries.mapNotNull { entry ->
            val link = entry.link?.trim().orEmpty()
            val title = entry.title?.trim().orEmpty()
            if (link.isEmpty() || title.isEmpty()) return@mapNotNull null
            FetchedArticle(
                sourceId = entry.uri?.trim()?.takeIf { it.isNotEmpty() && it != link },
                title = title,
                url = link,
                excerpt = entry.description?.value?.let(::stripHtml)?.takeIf { it.isNotEmpty() },
                publishedAt = entry.publishedDate?.time,
            )
        }
        etag = response.etag
        lastModified = response.lastModified
        return articles
    }

    private fun stripHtml(value: String): String =
        value.replace(HTML_TAG, " ").replace(WHITESPACE, " ").trim()

    companion object {
        private val HTML_TAG = Regex("<[^>]*>")
        private val WHITESPACE = Regex("\\s+")
    }
}
