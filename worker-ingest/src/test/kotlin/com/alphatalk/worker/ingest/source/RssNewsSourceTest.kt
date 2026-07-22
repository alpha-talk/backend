package com.alphatalk.worker.ingest.source

import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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

    private val source = RssNewsSource("test", "https://example.com/rss") {
        ByteArrayInputStream(rss.toByteArray())
    }

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
}
