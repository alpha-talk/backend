package com.alphatalk.worker.llm.persist

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.sql.Statement
import java.sql.Timestamp

@Repository
@Transactional
class JdbcStreamEventStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val mapper: ObjectMapper,
) : StreamEventStore {

    override fun insertEvents(events: List<StreamEventRow>): Set<String> {
        if (events.isEmpty()) return emptySet()
        val counts = jdbc.batchUpdate(
            """
            INSERT INTO stream_event (event_id, code, type, occurred_at, source, payload)
            VALUES (:eventId, :code, :type, :occurredAt, :source, CAST(:payload AS jsonb))
            ON CONFLICT DO NOTHING
            """,
            events.map {
                MapSqlParameterSource()
                    .addValue("eventId", it.eventId)
                    .addValue("code", it.code)
                    .addValue("type", it.type)
                    .addValue("occurredAt", Timestamp.from(it.occurredAt))
                    .addValue("source", it.source)
                    .addValue("payload", mapper.writeValueAsString(it.data))
            }.toTypedArray(),
        )
        check(counts.none { it == Statement.SUCCESS_NO_INFO }) {
            "배치 삽입이 행 수를 돌려주지 않아(SUCCESS_NO_INFO) 발행 대상을 판정할 수 없다 — " +
                "JDBC URL의 reWriteBatchedInserts 설정을 확인하라"
        }
        return events.filterIndexed { index, _ -> counts[index] > 0 }.mapTo(mutableSetOf(), StreamEventRow::eventId)
    }

    override fun refreshSources(eventIds: Collection<String>, sourcesJson: String) {
        if (eventIds.isEmpty()) return
        jdbc.update(
            """
            UPDATE stream_event
            SET payload = jsonb_set(payload, '{sources}', CAST(:sources AS jsonb))
            WHERE event_id IN (:eventIds)
            """,
            mapOf("eventIds" to eventIds, "sources" to sourcesJson),
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
