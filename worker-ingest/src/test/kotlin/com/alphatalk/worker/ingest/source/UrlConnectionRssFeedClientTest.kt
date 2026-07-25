package com.alphatalk.worker.ingest.source

import com.sun.net.httpserver.HttpServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class UrlConnectionRssFeedClientTest {
    private lateinit var server: HttpServer

    @BeforeEach
    fun setUp() {
        server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    }

    @AfterEach
    fun tearDown() {
        server.stop(0)
    }

    @Test
    fun `응답 validator를 다음 요청 헤더로 보내고 304를 처리한다`() {
        val conditionalRequestSeen = AtomicBoolean()
        val body = """
            <?xml version="1.0" encoding="UTF-8"?>
            <rss version="2.0">
              <channel>
                <title>경제</title>
                <link>https://example.com</link>
                <description>feed</description>
                <item>
                  <title>삼성전자 수주</title>
                  <link>https://example.com/news/1</link>
                </item>
              </channel>
            </rss>
        """.trimIndent().toByteArray()
        server.createContext("/rss") { exchange ->
            val etag = exchange.requestHeaders.getFirst("If-None-Match")
            val modifiedSince = exchange.requestHeaders.getFirst("If-Modified-Since")
            if (etag == "\"v1\"" && modifiedSince != null) {
                conditionalRequestSeen.set(true)
                exchange.sendResponseHeaders(304, -1)
            } else {
                exchange.responseHeaders.add("Content-Type", "application/rss+xml")
                exchange.responseHeaders.add("ETag", "\"v1\"")
                exchange.responseHeaders.add("Last-Modified", "Tue, 14 Nov 2023 22:13:20 GMT")
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            exchange.close()
        }
        server.start()
        val source = RssNewsSource(
            id = "test-economy",
            name = "test",
            feedUrl = "http://127.0.0.1:${server.address.port}/rss",
            client = UrlConnectionRssFeedClient(),
        )

        assertEquals(1, source.fetchLatest().size)
        assertTrue(source.fetchLatest().isEmpty())
        assertTrue(conditionalRequestSeen.get())
    }
}
