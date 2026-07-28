package com.alphatalk.coreapi.auth

import java.time.Instant

data class RefreshTokenRecord(
    val userId: Long,
    val tokenHash: String,
    val expiresAt: Instant,
    val revokedAt: Instant?,
)

interface RefreshTokenStore {
    fun save(userId: Long, tokenHash: String, expiresAt: Instant, rotatedFrom: String?)
    fun find(tokenHash: String): RefreshTokenRecord?
    fun revoke(tokenHash: String, at: Instant): Boolean
    fun revokeAllOf(userId: Long, at: Instant): Int
}
