package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Instant

@Entity(name = "StreamEventWrite")
@Table(name = "stream_event")
class StreamEventWriteEntity(
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

interface StreamEventWriteJpaRepository : JpaRepository<StreamEventWriteEntity, String>

@Repository
class JpaStreamEventAppender(
    private val events: StreamEventWriteJpaRepository,
    private val mapper: ObjectMapper,
) : StreamEventAppender {
    override fun append(event: NewStreamEvent) {
        events.save(
            StreamEventWriteEntity(
                eventId = event.eventId,
                code = event.code,
                type = event.type.storedType,
                occurredAt = event.occurredAt,
                source = event.source,
                payload = event.payload,
            ),
        )
    }

    override fun markDeleted(eventId: String): Boolean {
        val entity = events.findById(eventId).orElse(null) ?: return false
        val payload = mapper.readTree(entity.payload)
        if (payload !is ObjectNode) return false
        payload.put("deleted", true)
        entity.payload = mapper.writeValueAsString(payload)
        events.save(entity)
        return true
    }
}
