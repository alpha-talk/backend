package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.envelope.StreamCategory
import com.fasterxml.jackson.databind.ObjectMapper
import com.github.f4b6a3.ulid.UlidCreator
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.time.Instant

@Entity
@Table(name = "stream_event")
class StreamEventEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "event_id", nullable = false, length = 26)
    val eventId: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "type", nullable = false)
    val type: String = "",
    @Column(name = "occurred_at", nullable = false)
    val occurredAt: Instant = Instant.EPOCH,
    @Column(name = "source")
    val source: String? = null,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false)
    val payload: String = "{}",
    @Column(name = "created_at", nullable = false)
    val createdAt: Instant = Instant.EPOCH,
    @Column(name = "source_key")
    val sourceKey: String? = null,
)

interface StreamEventJpaRepository : JpaRepository<StreamEventEntity, String> {
    fun findBySourceKey(sourceKey: String): StreamEventEntity?
}

@Repository
class JpaOpinionEventBinder(
    private val events: StreamEventJpaRepository,
    private val opinions: InvestOpinionJpaRepository,
    private val mapper: ObjectMapper,
    private val clock: () -> Instant = Instant::now,
) : OpinionEventBinder {

    override fun ensureEvent(opinion: UnpublishedOpinion): String {
        opinion.streamEventId?.let { return it }
        val observation = opinion.observation
        val eventId = events.findBySourceKey(observation.sourceKey)?.eventId ?: insertNew(observation)
        opinions.linkEvent(observation.key(), eventId)
        return eventId
    }

    private fun insertNew(observation: OpinionObservation): String = try {
        val eventId = UlidCreator.getMonotonicUlid().toString()
        events.save(
            StreamEventEntity(
                eventId = eventId,
                code = observation.code,
                type = StreamCategory.REPORT.eventType,
                occurredAt = observation.collectedAt,
                source = observation.brokerCode,
                payload = mapper.writeValueAsString(observation.toStreamData()),
                createdAt = clock(),
                sourceKey = observation.sourceKey,
            ),
        )
        eventId
    } catch (e: DataIntegrityViolationException) {
        events.findBySourceKey(observation.sourceKey)?.eventId ?: throw e
    }
}
