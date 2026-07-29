package com.alphatalk.coreapi.stream

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class JdbcStreamStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val mapper: ObjectMapper,
) : StreamStore {
    override fun find(query: StreamQuery): List<StreamItem> {
        val params = MapSqlParameterSource()
            .addValue("code", query.code)
            .addValue("limit", query.limit)
        val conditions = mutableListOf("code = :code")
        appendTypeFilter(query.types, conditions, params)
        query.cursor?.let {
            params.addValue("cursor", it)
            conditions += if (query.direction == CursorDirection.AFTER) "event_id > :cursor" else "event_id < :cursor"
        }
        val order = if (query.direction == CursorDirection.AFTER) "ASC" else "DESC"
        val sql = """
            SELECT event_id, code, type, occurred_at, source, payload
            FROM stream_event
            WHERE ${conditions.joinToString(" AND ")}
            ORDER BY event_id $order
            LIMIT :limit
        """.trimIndent()
        return jdbc.query(sql, params, ::mapItem)
    }

    override fun hasOlderThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        exists(code, eventId, types, "event_id < :cursor")

    override fun hasNewerThan(code: String, eventId: String, types: List<StreamEventType>): Boolean =
        exists(code, eventId, types, "event_id > :cursor")

    private fun exists(code: String, eventId: String, types: List<StreamEventType>, comparison: String): Boolean {
        val params = MapSqlParameterSource()
            .addValue("code", code)
            .addValue("cursor", eventId)
        val conditions = mutableListOf("code = :code", comparison)
        appendTypeFilter(types, conditions, params)
        val sql = "SELECT exists(SELECT 1 FROM stream_event WHERE ${conditions.joinToString(" AND ")})"
        return jdbc.queryForObject(sql, params, Boolean::class.java) == true
    }

    private fun appendTypeFilter(
        types: List<StreamEventType>,
        conditions: MutableList<String>,
        params: MapSqlParameterSource,
    ) {
        if (types.isEmpty()) return
        conditions += "type IN (:types)"
        params.addValue("types", types.map(StreamEventType::storedType))
    }

    private fun mapItem(rows: ResultSet, rowNum: Int) = StreamItem(
        eventId = rows.getString("event_id").trim(),
        code = rows.getString("code").trim(),
        type = rows.getString("type"),
        occurredAt = rows.getTimestamp("occurred_at").toInstant().toEpochMilli(),
        source = rows.getString("source"),
        payload = mapper.readTree(rows.getString("payload")),
    )
}
