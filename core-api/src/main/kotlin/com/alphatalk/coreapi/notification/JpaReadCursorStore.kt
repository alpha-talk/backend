package com.alphatalk.coreapi.notification

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

data class ReadCursorId(
    val userId: Long = 0,
    val code: String = "",
) : Serializable

@Entity
@Table(name = "read_cursor")
@IdClass(ReadCursorId::class)
class ReadCursorEntity(
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "last_event_id", nullable = false, length = 26, columnDefinition = "char(26)")
    var lastEventId: String = "",
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

interface ReadCursorJpaRepository : JpaRepository<ReadCursorEntity, ReadCursorId> {
    fun findByUserIdAndCodeIn(userId: Long, codes: Collection<String>): List<ReadCursorEntity>

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update ReadCursorEntity cursor
        set cursor.lastEventId = :eventId, cursor.updatedAt = :at
        where cursor.userId = :userId and cursor.code = :code and cursor.lastEventId < :eventId
        """,
    )
    fun advanceIfNewer(
        @Param("userId") userId: Long,
        @Param("code") code: String,
        @Param("eventId") eventId: String,
        @Param("at") at: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        insert into ReadCursorEntity (userId, code, lastEventId, updatedAt)
        values (:userId, :code, :eventId, :at)
        on conflict do nothing
        """,
    )
    fun insertIfAbsent(
        @Param("userId") userId: Long,
        @Param("code") code: String,
        @Param("eventId") eventId: String,
        @Param("at") at: Instant,
    ): Int
}

@Repository
class JpaReadCursorStore(
    private val cursors: ReadCursorJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : ReadCursorStore {
    @Transactional(readOnly = true)
    override fun find(userId: Long, codes: Collection<String>): Map<String, String> {
        if (codes.isEmpty()) return emptyMap()
        return cursors.findByUserIdAndCodeIn(userId, codes)
            .associate { it.code.trim() to it.lastEventId.trim() }
    }

    override fun advance(userId: Long, code: String, eventId: String): Boolean {
        val now = clock.instant()
        if (cursors.advanceIfNewer(userId, code, eventId, now) > 0) return true
        if (cursors.insertIfAbsent(userId, code, eventId, now) > 0) return true
        return cursors.advanceIfNewer(userId, code, eventId, now) > 0
    }

    override fun advanceAll(userId: Long, cursors: Map<String, String>) {
        cursors.forEach { (code, eventId) -> advance(userId, code, eventId) }
    }
}
