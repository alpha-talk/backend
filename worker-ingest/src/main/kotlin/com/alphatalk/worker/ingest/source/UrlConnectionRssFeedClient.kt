package com.alphatalk.worker.ingest.source

import org.springframework.stereotype.Component
import java.io.IOException
import java.net.HttpURLConnection
import java.net.URI

@Component
class UrlConnectionRssFeedClient : RssFeedClient {
    override fun fetch(request: RssFeedRequest): RssFeedResponse {
        val connection = URI(request.url).toURL().openConnection() as HttpURLConnection
        connection.connectTimeout = 5_000
        connection.readTimeout = 10_000
        connection.instanceFollowRedirects = true
        connection.setRequestProperty("User-Agent", "AlphaTalkIngest/0.1")
        connection.setRequestProperty("Accept", "application/rss+xml, application/xml, text/xml, */*")
        request.etag?.let { connection.setRequestProperty("If-None-Match", it) }
        request.lastModified?.let { connection.ifModifiedSince = it }

        return try {
            when (val status = connection.responseCode) {
                HttpURLConnection.HTTP_NOT_MODIFIED -> RssFeedResponse.NotModified
                in 200..299 -> RssFeedResponse.Modified(
                    body = connection.inputStream.use { it.readBytes() },
                    etag = connection.getHeaderField("ETag"),
                    lastModified = connection.lastModified.takeIf { it > 0 },
                )
                else -> {
                    connection.errorStream?.close()
                    throw IOException("RSS fetch failed: status=$status")
                }
            }
        } finally {
            connection.disconnect()
        }
    }
}
