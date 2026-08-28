package com.alphatalk.coreapi.stream

import com.alphatalk.coreapi.stream.QStreamEventEntity.streamEventEntity
import com.fasterxml.jackson.databind.ObjectMapper
import com.querydsl.core.types.dsl.BooleanExpression
import com.querydsl.jpa.impl.JPAQueryFactory
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Entity
@Immutable
@Table(name = "stream_event")
class StreamEventEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "event_id", length = 26, columnDefinition = "char(26)")
    var eventId: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @Column(name = "type", nullable = false)
    var type: String = "",
    @Column(name = "occurred_at", nullable = false)
    var occurredAt: Instant = Instant.EPOCH,
    @Column(name = "source")
    var source: String? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    var payload: String = "",
)

interface StreamEventJpaRepository : JpaRepository<StreamEventEntity, String>

@Repository
@Transactional(readOnly = true)
class JpaStreamStore(
    private val events: StreamEventJpaRepository,
    private val queryFactory: JPAQueryFactory,
    private val mapper: ObjectMapper,
) : StreamStore {
    override fun find(query: StreamQuery): List<StreamItem> {
        val ascending = query.direction == CursorDirection.AFTER
        val order = if (ascending) streamEventEntity.eventId.asc() else streamEventEntity.eventId.desc()
        val cursorFilter = query.cursor?.let { if (ascending) newerThan(it) else olderThan(it) }
        return queryFactory.selectFrom(streamEventEntity)
            .where(inRoom(query.code), ofTypes(query.types), cursorFilter)
            .orderBy(order)
            .limit(query.limit.toLong())
            .fetch()
            .map(::toItem)
    }

    override fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        exists(inRoom(code), ofTypes(types), olderThan(eventId))

    override fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        exists(inRoom(code), ofTypes(types), newerThan(eventId))

    override fun findInRoom(code: String, eventId: String): StreamItem? =
        events.findById(eventId).orElse(null)
            ?.takeIf { it.code.trim() == code }
            ?.let(::toItem)

    private fun exists(vararg conditions: BooleanExpression?): Boolean =
        queryFactory.selectOne()
            .from(streamEventEntity)
            .where(*conditions)
            .fetchFirst() != null

    private fun inRoom(code: String): BooleanExpression = streamEventEntity.code.eq(code)

    private fun ofTypes(types: List<StreamEventType>): BooleanExpression? =
        if (types.isEmpty()) null else streamEventEntity.type.`in`(types.map(StreamEventType::storedType))

    private fun olderThan(eventId: String): BooleanExpression = streamEventEntity.eventId.lt(eventId)

    private fun newerThan(eventId: String): BooleanExpression = streamEventEntity.eventId.gt(eventId)

    private fun toItem(entity: StreamEventEntity) = StreamItem(
        eventId = entity.eventId.trim(),
        code = entity.code.trim(),
        type = entity.type,
        occurredAt = entity.occurredAt.toEpochMilli(),
        source = entity.source,
        payload = mapper.readTree(entity.payload),
    )
}
