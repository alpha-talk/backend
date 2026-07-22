package com.alphatalk.worker.ingest.source

import com.rometools.rome.io.SyndFeedInput
import com.rometools.rome.io.XmlReader
import java.io.InputStream
import java.net.URI

class RssNewsSource(
    override val name: String,
    private val feedUrl: String,
    private val open: (String) -> InputStream = ::defaultOpen,
) : NewsSource {

    override fun fetchLatest(): List<FetchedArticle> {
        val feed = open(feedUrl).use { SyndFeedInput().build(XmlReader(it)) }
        return feed.entries.mapNotNull { entry ->
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
    }

    private fun stripHtml(value: String): String =
        value.replace(HTML_TAG, " ").replace(WHITESPACE, " ").trim()

    companion object {
        private val HTML_TAG = Regex("<[^>]*>")
        private val WHITESPACE = Regex("\\s+")

        private fun defaultOpen(url: String): InputStream {
            val connection = URI(url).toURL().openConnection()
            connection.connectTimeout = 5_000
            connection.readTimeout = 10_000
            connection.setRequestProperty("User-Agent", "AlphaTalkIngest/0.1")
            return connection.getInputStream()
        }
    }
}
