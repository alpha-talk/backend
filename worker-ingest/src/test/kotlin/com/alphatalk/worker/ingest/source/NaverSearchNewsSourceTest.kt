package com.alphatalk.worker.ingest.source

import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class NaverSearchNewsSourceTest {
    private val response = """
        {
          "items": [
            {
              "title": "<b>삼성전자</b>, 3나노 &quot;대규모&quot; 수주",
              "originallink": "https://example.com/news/1",
              "link": "https://n.news.naver.com/article/1",
              "description": "<b>삼성전자</b>가 수주에 성공했다.",
              "pubDate": "Thu, 16 Jul 2026 09:00:00 +0900"
            },
            {
              "title": "링크 없는 항목",
              "originallink": "",
              "link": "",
              "description": "버려져야 한다",
              "pubDate": "invalid"
            }
          ]
        }
    """.trimIndent()

    private val requested = mutableListOf<Pair<String, Map<String, String>>>()
    private val source = NaverSearchNewsSource(
        clientId = "id",
        clientSecret = "secret",
        queries = listOf(NaverSearchNewsSource.StockQuery("005930", "삼성전자")),
        display = 30,
    ) { url, headers ->
        requested.add(url to headers)
        ByteArrayInputStream(response.toByteArray())
    }

    @Test
    fun `검색 결과 파싱 - 태그 제거·원문 링크 우선·종목 코드 부착`() {
        val articles = source.fetchLatest()
        assertEquals(1, articles.size)

        val article = articles.single()
        assertEquals("삼성전자, 3나노 \"대규모\" 수주", article.title)
        assertEquals("https://example.com/news/1", article.url)
        assertEquals("삼성전자가 수주에 성공했다.", article.excerpt)
        assertEquals(listOf("005930"), article.codes)
        assertNotNull(article.publishedAt)
    }

    @Test
    fun `여러 종목 쿼리에서 같은 기사가 나오면 코드 합집합으로 병합`() {
        val merged = NaverSearchNewsSource(
            clientId = "id",
            clientSecret = "secret",
            queries = listOf(
                NaverSearchNewsSource.StockQuery("005930", "삼성전자"),
                NaverSearchNewsSource.StockQuery("000660", "SK하이닉스"),
            ),
            display = 30,
        ) { _, _ -> java.io.ByteArrayInputStream(response.toByteArray()) }

        val articles = merged.fetchLatest()
        assertEquals(1, articles.size)
        assertEquals(listOf("000660", "005930"), articles.single().codes)
    }

    @Test
    fun `요청에 인증 헤더와 쿼리 인코딩 포함`() {
        source.fetchLatest()
        val (url, headers) = requested.single()
        assertEquals("id", headers["X-Naver-Client-Id"])
        assertEquals("secret", headers["X-Naver-Client-Secret"])
        assertEquals(true, url.contains("query=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90"))
        assertEquals(true, url.contains("sort=date"))
    }
}
