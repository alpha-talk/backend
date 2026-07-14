package com.alphatalk.auth

import io.jsonwebtoken.JwtException
import io.jsonwebtoken.Jwts
import io.jsonwebtoken.security.Keys
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.util.Date

class JwtTokenProvider(
    secret: String,
    private val clock: Clock = Clock.systemUTC(),
) : TokenIssuer, TokenVerifier {
    init {
        require(secret.toByteArray(StandardCharsets.UTF_8).size >= 32) {
            "HS256 secret must be at least 32 bytes"
        }
    }

    private val key = Keys.hmacShaKeyFor(secret.toByteArray(StandardCharsets.UTF_8))
    private val parser = Jwts.parser().verifyWith(key).build()

    override fun issue(userId: Long, ttl: Duration): String {
        val now = clock.instant()
        return Jwts.builder()
            .subject(userId.toString())
            .issuedAt(Date.from(now))
            .expiration(Date.from(now + ttl))
            .signWith(key)
            .compact()
    }

    override fun verify(token: String): Long = try {
        val claims = parser.parseSignedClaims(token).payload
        if (claims.expiration == null) throw InvalidTokenException("token has no expiration")
        claims.subject?.toLongOrNull() ?: throw InvalidTokenException("subject is not a user id")
    } catch (e: InvalidTokenException) {
        throw e
    } catch (e: JwtException) {
        throw InvalidTokenException("token rejected", e)
    } catch (e: IllegalArgumentException) {
        throw InvalidTokenException("token malformed", e)
    }
}
