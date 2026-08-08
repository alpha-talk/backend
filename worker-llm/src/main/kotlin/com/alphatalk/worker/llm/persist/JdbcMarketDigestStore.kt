package com.alphatalk.worker.llm.persist

import com.alphatalk.contracts.envelope.MarketAnalysis
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.dao.EmptyResultDataAccessException
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Date
import java.time.LocalDate

@Repository
class JdbcMarketDigestStore(
    private val jdbc: NamedParameterJdbcTemplate,
    private val mapper: ObjectMapper,
) : MarketDigestStore {

    override fun find(date: String): MarketAnalysis? =
        try {
            jdbc.queryForObject(
                "SELECT payload FROM market_digest WHERE date = :date",
                mapOf("date" to Date.valueOf(LocalDate.parse(date))),
                String::class.java,
            )?.let { mapper.readValue(it, MarketAnalysis::class.java) }
        } catch (missing: EmptyResultDataAccessException) {
            null
        }

    override fun save(date: String, analysis: MarketAnalysis): Boolean =
        jdbc.update(
            """
            INSERT INTO market_digest (date, payload, degraded)
            VALUES (:date, CAST(:payload AS jsonb), :degraded)
            ON CONFLICT (date) DO UPDATE
            SET payload = EXCLUDED.payload, degraded = EXCLUDED.degraded, created_at = now()
            WHERE market_digest.degraded = true AND EXCLUDED.degraded = false
            """,
            mapOf(
                "date" to Date.valueOf(LocalDate.parse(date)),
                "payload" to mapper.writeValueAsString(analysis),
                "degraded" to analysis.degraded,
            ),
        ) > 0
}
