package com.alphatalk.worker.llm.persist

import com.alphatalk.contracts.envelope.StreamData
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import java.sql.Timestamp
import java.time.Instant

class JdbcStreamEventStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val mapper: ObjectMapper,
) : StreamEventStore {

    override fun insertEvent(
        eventId: String,
        code: String,
        type: String,
        occurredAt: Instant,
        source: String?,
        data: StreamData,
    ): Boolean =
        jdbc.update(
            """
            INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
            VALUES (:eventId, :code, :type, :occurredAt, :source, CAST(:payload AS jsonb))
            ON CONFLICT (event_id) DO NOTHING
            """,
            mapOf(
                "eventId" to eventId,
                "code" to code,
                "type" to type,
                "occurredAt" to Timestamp.from(occurredAt),
                "source" to source,
                "payload" to mapper.writeValueAsString(data),
            ),
        ) > 0

    override fun refreshSources(eventId: String, sourcesJson: String) {
        jdbc.update(
            """
            UPDATE stream_event
            SET payload = jsonb_set(payload, '{sources}', CAST(:sources AS jsonb))
            WHERE event_id = :eventId
            """,
            mapOf("eventId" to eventId, "sources" to sourcesJson),
        )
    }

    override fun digestExists(code: String, date: String): Boolean =
        jdbc.queryForObject(
            """
            SELECT count(*) FROM stream_event
            WHERE code = :code AND type = 'AI' AND payload -> 'digest' ->> 'date' = :date
            """,
            mapOf("code" to code, "date" to date),
            Long::class.java,
        )!! > 0
}
