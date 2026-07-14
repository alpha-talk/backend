package com.alphatalk.auth

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class JwtTokenProviderTest {
    private val secret = "test-secret-must-be-32-bytes-minimum!!"
    private val provider = JwtTokenProvider(secret)

    @Test
    fun `발급-검증 왕복`() {
        val token = provider.issue(userId = 42L, ttl = Duration.ofMinutes(5))
        assertEquals(42L, provider.verify(token))
    }

    @Test
    fun `만료 토큰 - InvalidTokenException`() {
        val past = Clock.fixed(Instant.now().minus(Duration.ofHours(1)), ZoneOffset.UTC)
        val expired = JwtTokenProvider(secret, past).issue(42L, Duration.ofMinutes(5))
        assertFailsWith<InvalidTokenException> { provider.verify(expired) }
    }

    @Test
    fun `다른 시크릿으로 서명된 토큰 - 거부`() {
        val other = JwtTokenProvider("another-secret-also-32-bytes-long!!!!!")
        val token = other.issue(42L, Duration.ofMinutes(5))
        assertFailsWith<InvalidTokenException> { provider.verify(token) }
    }

    @Test
    fun `형식이 깨진 토큰 - 거부`() {
        assertFailsWith<InvalidTokenException> { provider.verify("not-a-jwt") }
        assertFailsWith<InvalidTokenException> { provider.verify("") }
        assertFailsWith<InvalidTokenException> { provider.verify("a.b.c") }
    }

    @Test
    fun `subject가 숫자가 아니면 - 거부`() {
        val key = io.jsonwebtoken.security.Keys.hmacShaKeyFor(secret.toByteArray())
        val token = io.jsonwebtoken.Jwts.builder()
            .subject("not-a-number")
            .expiration(java.util.Date(System.currentTimeMillis() + 60_000))
            .signWith(key)
            .compact()
        assertFailsWith<InvalidTokenException> { provider.verify(token) }
    }

    @Test
    fun `exp 없는 토큰 - 거부 (액세스 토큰은 만료 필수)`() {
        val key = io.jsonwebtoken.security.Keys.hmacShaKeyFor(secret.toByteArray())
        val token = io.jsonwebtoken.Jwts.builder().subject("42").signWith(key).compact()
        assertFailsWith<InvalidTokenException> { provider.verify(token) }
    }

    @Test
    fun `짧은 시크릿 - 생성 거부`() {
        assertFailsWith<IllegalArgumentException> { JwtTokenProvider("too-short") }
    }
}
