package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.domain.Specification
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.JpaSpecificationExecutor
import org.springframework.stereotype.Repository
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

interface StreamEventJpaRepository :
    JpaRepository<StreamEventEntity, String>,
    JpaSpecificationExecutor<StreamEventEntity>

@Repository
class JpaStreamStore(
    private val events: StreamEventJpaRepository,
    private val mapper: ObjectMapper,
) : StreamStore {
    override fun find(query: StreamQuery): List<StreamItem> {
        val ascending = query.direction == CursorDirection.AFTER
        val spec = inRoom(query.code)
            .and(ofTypes(query.types))
            .and(query.cursor?.let { if (ascending) newerThan(it) else olderThan(it) })
        val order = if (ascending) Sort.Direction.ASC else Sort.Direction.DESC
        return events.findBy<StreamEventEntity, List<StreamEventEntity>>(spec) {
            it.sortBy(Sort.by(order, "eventId"))
                .limit(query.limit)
                .all()
        }
            .map(::toItem)
    }

    override fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        events.exists(inRoom(code).and(ofTypes(types)).and(olderThan(eventId)))

    override fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        events.exists(inRoom(code).and(ofTypes(types)).and(newerThan(eventId)))

    override fun findInRoom(code: String, eventId: String): StreamItem? =
        events.findById(eventId).orElse(null)
            ?.takeIf { it.code.trim() == code }
            ?.let(::toItem)

    private fun inRoom(code: String) = Specification<StreamEventEntity> { root, _, builder ->
        builder.equal(root.get<String>("code"), code)
    }

    private fun ofTypes(types: List<StreamEventType>) = Specification<StreamEventEntity> { root, _, _ ->
        if (types.isEmpty()) null else root.get<String>("type").`in`(types.map(StreamEventType::storedType))
    }

    private fun olderThan(eventId: String) = Specification<StreamEventEntity> { root, _, builder ->
        builder.lessThan(root.get("eventId"), eventId)
    }

    private fun newerThan(eventId: String) = Specification<StreamEventEntity> { root, _, builder ->
        builder.greaterThan(root.get("eventId"), eventId)
    }

    private fun toItem(entity: StreamEventEntity) = StreamItem(
        eventId = entity.eventId.trim(),
        code = entity.code.trim(),
        type = entity.type,
        occurredAt = entity.occurredAt.toEpochMilli(),
        source = entity.source,
        payload = mapper.readTree(entity.payload),
    )
}
