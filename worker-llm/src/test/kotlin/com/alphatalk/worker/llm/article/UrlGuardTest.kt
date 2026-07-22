package com.alphatalk.worker.llm.article

import org.junit.jupiter.api.Test
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class UrlGuardTest {
    @Test
    fun `공인 IP·정상 스킴은 허용`() {
        assertNotNull(UrlGuard.safeUrl("http://93.184.216.34/news/1"))
        assertNotNull(UrlGuard.safeUrl("https://8.8.8.8/article?id=1"))
    }

    @Test
    fun `http·https 외 스킴 거부`() {
        assertNull(UrlGuard.safeUrl("ftp://93.184.216.34/x"))
        assertNull(UrlGuard.safeUrl("file:///etc/passwd"))
    }

    @Test
    fun `루프백·사설·링크로컬·메타데이터 대역 거부`() {
        assertNull(UrlGuard.safeUrl("http://localhost/admin"))
        assertNull(UrlGuard.safeUrl("http://127.0.0.1/admin"))
        assertNull(UrlGuard.safeUrl("http://10.0.0.5/internal"))
        assertNull(UrlGuard.safeUrl("http://172.16.0.1/internal"))
        assertNull(UrlGuard.safeUrl("http://192.168.1.2/router"))
        assertNull(UrlGuard.safeUrl("http://169.254.169.254/latest/meta-data/"))
        assertNull(UrlGuard.safeUrl("http://100.64.0.1/cgnat"))
        assertNull(UrlGuard.safeUrl("http://[::1]/admin"))
        assertNull(UrlGuard.safeUrl("http://[fd00::1]/internal"))
    }

    @Test
    fun `호스트 없는 URL·비정상 문자열 거부`() {
        assertNull(UrlGuard.safeUrl("http:///path-only"))
        assertNull(UrlGuard.safeUrl("not a url"))
    }
}
