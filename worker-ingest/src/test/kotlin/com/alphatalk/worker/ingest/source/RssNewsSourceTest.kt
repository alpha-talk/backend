package com.alphatalk.worker.ingest.source

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RssNewsSourceTest {
    private val rss = """
        <?xml version="1.0" encoding="UTF-8"?>
        <rss version="2.0">
          <channel>
            <title>테스트 경제</title>
            <link>https://example.com</link>
            <description>feed</description>
            <item>
              <title> 삼성전자, 대규모 수주 </title>
              <link>https://example.com/news/1?utm_source=rss</link>
              <guid isPermaLink="false">news-0001</guid>
              <description><![CDATA[<p>본문 <b>발췌</b>입니다.</p>]]></description>
              <pubDate>Wed, 16 Jul 2026 09:00:00 +0900</pubDate>
            </item>
            <item>
              <title>링크 없는 항목</title>
              <description>버려져야 한다</description>
            </item>
            <item>
              <title>guid가 링크와 동일</title>
              <link>https://example.com/news/2</link>
              <guid>https://example.com/news/2</guid>
            </item>
          </channel>
        </rss>
    """.trimIndent()

    private val source = RssNewsSource(
        id = "test-economy",
        name = "test",
        feedUrl = "https://example.com/rss",
        client = RssFeedClient {
            RssFeedResponse.Modified(rss.toByteArray(), null, null)
        },
    )

    @Test
    fun `RSS 파싱 - 제목 트림·HTML 제거·guid·발행시각`() {
        val articles = source.fetchLatest()
        assertEquals(2, articles.size)

        val first = articles.first()
        assertEquals("삼성전자, 대규모 수주", first.title)
        assertEquals("https://example.com/news/1?utm_source=rss", first.url)
        assertEquals("news-0001", first.sourceId)
        assertEquals("본문 발췌 입니다.", first.excerpt)
        assertNotNull(first.publishedAt)
    }

    @Test
    fun `guid가 링크와 같으면 sourceId로 쓰지 않는다`() {
        assertNull(source.fetchLatest().last().sourceId)
    }

    @Test
    fun `ETag와 Last-Modified를 다음 요청에 전달하고 304는 빈 결과`() {
        val requests = mutableListOf<RssFeedRequest>()
        val conditionalSource = RssNewsSource(
            id = "test-economy",
            name = "test",
            feedUrl = "https://example.com/rss",
            client = RssFeedClient { request ->
                requests += request
                if (requests.size == 1) {
                    RssFeedResponse.Modified(rss.toByteArray(), "\"v1\"", 1_700_000_000_000)
                } else {
                    RssFeedResponse.NotModified
                }
            },
        )

        assertEquals(2, conditionalSource.fetchLatest().size)
        assertTrue(conditionalSource.fetchLatest().isEmpty())
        assertEquals(null, requests.first().etag)
        assertEquals(null, requests.first().lastModified)
        assertEquals("\"v1\"", requests.last().etag)
        assertEquals(1_700_000_000_000, requests.last().lastModified)
    }
}
