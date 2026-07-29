package com.alphatalk.coreapi.auth

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.GeneratedValue
import jakarta.persistence.GenerationType
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Entity
@Table(name = "refresh_tokens")
class RefreshTokenEntity(
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    var id: Long? = null,
    @Column(name = "user_id", nullable = false)
    var userId: Long = 0,
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "token_hash", nullable = false, unique = true, length = 64, columnDefinition = "char(64)")
    var tokenHash: String = "",
    @Column(name = "expires_at", nullable = false)
    var expiresAt: Instant = Instant.EPOCH,
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "rotated_from", length = 64, columnDefinition = "char(64)")
    var rotatedFrom: String? = null,
    @Column(name = "revoked_at")
    var revokedAt: Instant? = null,
)

interface RefreshTokenJpaRepository : JpaRepository<RefreshTokenEntity, Long> {
    fun findByTokenHash(tokenHash: String): RefreshTokenEntity?

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update RefreshTokenEntity token
        set token.revokedAt = :at
        where token.tokenHash = :tokenHash and token.revokedAt is null
        """,
    )
    fun revoke(
        @Param("tokenHash") tokenHash: String,
        @Param("at") at: Instant,
    ): Int

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update RefreshTokenEntity token
        set token.revokedAt = :at
        where token.userId = :userId and token.revokedAt is null
        """,
    )
    fun revokeAllOf(
        @Param("userId") userId: Long,
        @Param("at") at: Instant,
    ): Int
}

@Repository
class JpaRefreshTokenStore(
    private val tokens: RefreshTokenJpaRepository,
) : RefreshTokenStore {
    override fun save(userId: Long, tokenHash: String, expiresAt: Instant, rotatedFrom: String?) {
        tokens.save(
            RefreshTokenEntity(
                userId = userId,
                tokenHash = tokenHash,
                expiresAt = expiresAt,
                rotatedFrom = rotatedFrom,
            ),
        )
    }

    override fun find(tokenHash: String): RefreshTokenRecord? =
        tokens.findByTokenHash(tokenHash)?.toRecord()

    @Transactional
    override fun revoke(tokenHash: String, at: Instant): Boolean =
        tokens.revoke(tokenHash, at) > 0

    @Transactional
    override fun revokeAllOf(userId: Long, at: Instant): Int =
        tokens.revokeAllOf(userId, at)

    private fun RefreshTokenEntity.toRecord() = RefreshTokenRecord(
        userId = userId,
        tokenHash = tokenHash,
        expiresAt = expiresAt,
        revokedAt = revokedAt,
    )
}
