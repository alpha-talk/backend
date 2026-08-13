package com.alphatalk.coreapi.notification

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.data.repository.findByIdOrNull
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Clock
import java.time.Instant

@Entity
@Table(name = "opinion_read_cursor")
class OpinionReadCursorEntity(
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "last_event_id", nullable = false, length = 26, columnDefinition = "char(26)")
    var lastEventId: String = "",
    @Column(name = "updated_at", nullable = false)
    var updatedAt: Instant = Instant.EPOCH,
)

interface OpinionReadCursorJpaRepository : JpaRepository<OpinionReadCursorEntity, Long> {
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        update OpinionReadCursorEntity cursor
        set cursor.lastEventId = :eventId, cursor.updatedAt = :at
        where cursor.userId = :userId and cursor.lastEventId < :eventId
        """,
    )
    fun advanceIfNewer(
        @Param("userId") userId: Long,
        @Param("eventId") eventId: String,
        @Param("at") at: Instant,
    ): Int

    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query(
        """
        insert into OpinionReadCursorEntity (userId, lastEventId, updatedAt)
        values (:userId, :eventId, :at)
        on conflict do nothing
        """,
    )
    fun insertIfAbsent(
        @Param("userId") userId: Long,
        @Param("eventId") eventId: String,
        @Param("at") at: Instant,
    ): Int
}

@Repository
class JpaOpinionReadCursorStore(
    private val cursors: OpinionReadCursorJpaRepository,
    private val clock: Clock = Clock.systemUTC(),
) : OpinionReadCursorStore {
    @Transactional(readOnly = true)
    override fun find(userId: Long): String? = cursors.findByIdOrNull(userId)?.lastEventId?.trim()

    override fun advance(userId: Long, eventId: String): String {
        val now = clock.instant()
        if (cursors.advanceIfNewer(userId, eventId, now) > 0) return eventId
        if (cursors.insertIfAbsent(userId, eventId, now) > 0) return eventId
        if (cursors.advanceIfNewer(userId, eventId, now) > 0) return eventId
        return find(userId) ?: eventId
    }
}
