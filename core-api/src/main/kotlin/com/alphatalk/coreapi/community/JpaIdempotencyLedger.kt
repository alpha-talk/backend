package com.alphatalk.coreapi.community

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.time.Clock
import java.time.Instant

data class IdempotencyRecordId(
    val userId: Long = 0,
    val idemKey: String = "",
) : Serializable

@Entity
@Table(name = "idempotency_record")
@IdClass(IdempotencyRecordId::class)
class IdempotencyRecordEntity(
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "idem_key", length = 26, columnDefinition = "char(26)")
    var idemKey: String = "",
    @Column(name = "action", nullable = false)
    var action: String = "",
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response", columnDefinition = "jsonb")
    var response: String? = null,
    @Column(name = "created_at", nullable = false)
    var createdAt: Instant = Instant.EPOCH,
)

interface IdempotencyRecordJpaRepository : JpaRepository<IdempotencyRecordEntity, IdempotencyRecordId> {
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        insert into IdempotencyRecordEntity (userId, idemKey, action, response, createdAt)
        values (:userId, :key, :action, :response, :at)
        on conflict do nothing
        """,
    )
    fun claim(
        @Param("userId") userId: Long,
        @Param("key") key: String,
        @Param("action") action: String,
        @Param("response") response: String?,
        @Param("at") at: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update IdempotencyRecordEntity r
        set r.response = :response
        where r.userId = :userId and r.idemKey = :key
        """,
    )
    fun complete(
        @Param("userId") userId: Long,
        @Param("key") key: String,
        @Param("response") response: String,
    ): Int
}

@Repository
class JpaIdempotencyLedger(
    private val records: IdempotencyRecordJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : IdempotencyLedger {
    @Transactional(readOnly = true)
    override fun find(userId: Long, key: String): IdempotencyRecord? =
        records.findById(IdempotencyRecordId(userId, key)).orElse(null)
            ?.let { IdempotencyRecord(action = it.action, response = it.response) }

    override fun claim(userId: Long, key: String, action: String): Boolean =
        records.claim(userId, key, action, null, clock.instant()) > 0

    override fun complete(userId: Long, key: String, response: String) {
        records.complete(userId, key, response)
    }
}
