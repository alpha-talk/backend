package com.alphatalk.coreapi.search

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class JdbcStockSearchStore(
    private val jdbc: JdbcTemplate,
) : StockSearchStore {
    override fun search(query: String, limit: Int): List<StockSummary> {
        val escaped = escapeLike(query)
        val prefix = "$escaped%"
        val contains = "%$escaped%"
        return jdbc.query(SEARCH_SQL, ::mapSummary, prefix, contains, prefix, prefix, limit)
    }

    private fun mapSummary(rows: ResultSet, rowNum: Int) = StockSummary(
        code = rows.getString("code").trim(),
        name = rows.getString("name"),
        market = rows.getString("market"),
    )

    private fun escapeLike(value: String): String =
        value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    companion object {
        private val SEARCH_SQL = """
            SELECT code, name, market
            FROM stock_master
            WHERE is_active
              AND (code LIKE ? ESCAPE '\' OR name ILIKE ? ESCAPE '\')
            ORDER BY
                CASE
                    WHEN code LIKE ? ESCAPE '\' THEN 0
                    WHEN name ILIKE ? ESCAPE '\' THEN 1
                    ELSE 2
                END,
                shares_outstanding DESC NULLS LAST,
                code
            LIMIT ?
        """.trimIndent()
    }
}
