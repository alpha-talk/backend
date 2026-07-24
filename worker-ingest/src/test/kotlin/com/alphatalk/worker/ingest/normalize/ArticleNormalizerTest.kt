package com.alphatalk.worker.ingest.normalize

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals

class ArticleNormalizerTest {
    @Test
    fun `추적 파라미터 제거·호스트 소문자화·프래그먼트 제거`() {
        assertEquals(
            "https://example.com/News/1?id=3",
            ArticleNormalizer.normalizeUrl("HTTPS://Example.COM/News/1?utm_source=rss&id=3&fbclid=x#comment"),
        )
    }

    @Test
    fun `쿼리가 전부 추적 파라미터면 물음표도 제거`() {
        assertEquals(
            "https://example.com/news/1",
            ArticleNormalizer.normalizeUrl("https://example.com/news/1?utm_source=rss&utm_medium=feed"),
        )
    }

    @Test
    fun `URI로 해석 불가하면 트림만 하고 원문 유지`() {
        assertEquals("not a url", ArticleNormalizer.normalizeUrl("  not a url "))
    }

    @Test
    fun `소스 제공 ID 우선`() {
        assertEquals(
            "hankyung:news-0001",
            ArticleNormalizer.sourceId("hankyung", "news-0001", "https://example.com/1"),
        )
    }

    @Test
    fun `ID 없으면 정규화 URL 해시 - 같은 URL은 같은 ID`() {
        val a = ArticleNormalizer.sourceId("hankyung", null, "https://example.com/1")
        val b = ArticleNormalizer.sourceId("hankyung", null, "https://example.com/1")
        val c = ArticleNormalizer.sourceId("hankyung", null, "https://example.com/2")
        assertEquals(a, b)
        assertNotEquals(a, c)
        assertEquals("hankyung:", a.substringBefore(a.removePrefix("hankyung:")))
        assertEquals(16, a.removePrefix("hankyung:").length)
    }
}
