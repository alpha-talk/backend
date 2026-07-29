package com.alphatalk.coreapi.subscription

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.ResultSet

@Repository
class JdbcWatchlistStore(
    private val jdbc: JdbcTemplate,
) : WatchlistStore {
    override fun list(userId: Long): List<WatchlistItem> = jdbc.query(LIST_SQL, ::mapItem, userId)

    override fun state(userId: Long, code: String): WatchlistState =
        jdbc.query(STATE_SQL, ::mapState, code, userId).single()

    override fun add(userId: Long, code: String): Boolean = jdbc.update(ADD_SQL, userId, code) == 1

    override fun remove(userId: Long, code: String): Boolean = jdbc.update(REMOVE_SQL, userId, code) == 1

    private fun mapItem(rows: ResultSet, rowNum: Int) = WatchlistItem(
        code = rows.getString("code").trim(),
        name = rows.getString("name"),
        market = rows.getString("market"),
        subscribedAt = rows.getTimestamp("created_at").time,
    )

    private fun mapState(rows: ResultSet, rowNum: Int) = WatchlistState(
        total = rows.getInt("total"),
        subscribed = rows.getBoolean("subscribed"),
    )

    companion object {
        private val LIST_SQL = """
            SELECT w.code, m.name, m.market, w.created_at
            FROM watchlist w
            JOIN stock_master m ON m.code = w.code
            WHERE w.user_id = ?
            ORDER BY w.created_at DESC, w.code
        """.trimIndent()

        private val STATE_SQL = """
            SELECT count(*) AS total,
                   count(*) FILTER (WHERE code = ?) > 0 AS subscribed
            FROM watchlist
            WHERE user_id = ?
        """.trimIndent()

        private val ADD_SQL = """
            INSERT INTO watchlist (user_id, code)
            VALUES (?, ?)
            ON CONFLICT DO NOTHING
        """.trimIndent()

        private val REMOVE_SQL = "DELETE FROM watchlist WHERE user_id = ? AND code = ?"
    }
}

@Repository
class JdbcStockCatalog(
    private val jdbc: JdbcTemplate,
) : StockCatalog {
    override fun exists(code: String): Boolean =
        jdbc.query(EXISTS_SQL, { rows, _ -> rows.getBoolean(1) }, code).single()

    companion object {
        private val EXISTS_SQL = "SELECT EXISTS (SELECT 1 FROM stock_master WHERE code = ? AND is_active)"
    }
}
