package com.alphatalk.coreapi.auth

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet
import java.sql.Timestamp
import java.time.Instant

@Repository
class JdbcRefreshTokenStore(
    private val jdbc: JdbcTemplate,
) : RefreshTokenStore {
    override fun save(userId: Long, tokenHash: String, expiresAt: Instant, rotatedFrom: String?) {
        jdbc.update(
            "INSERT INTO refresh_tokens (user_id, token_hash, expires_at, rotated_from) VALUES (?, ?, ?, ?)",
            userId,
            tokenHash,
            Timestamp.from(expiresAt),
            rotatedFrom,
        )
    }

    override fun find(tokenHash: String): RefreshTokenRecord? =
        jdbc.query(
            "SELECT user_id, token_hash, expires_at, revoked_at FROM refresh_tokens WHERE token_hash = ?",
            ::mapToken,
            tokenHash,
        ).firstOrNull()

    override fun revoke(tokenHash: String, at: Instant): Boolean =
        jdbc.update(
            "UPDATE refresh_tokens SET revoked_at = ? WHERE token_hash = ? AND revoked_at IS NULL",
            Timestamp.from(at),
            tokenHash,
        ) > 0

    override fun revokeAllOf(userId: Long, at: Instant): Int =
        jdbc.update(
            "UPDATE refresh_tokens SET revoked_at = ? WHERE user_id = ? AND revoked_at IS NULL",
            Timestamp.from(at),
            userId,
        )

    private fun mapToken(rows: ResultSet, rowNum: Int) = RefreshTokenRecord(
        userId = rows.getLong("user_id"),
        tokenHash = rows.getString("token_hash"),
        expiresAt = rows.getTimestamp("expires_at").toInstant(),
        revokedAt = rows.getTimestamp("revoked_at")?.toInstant(),
    )
}
