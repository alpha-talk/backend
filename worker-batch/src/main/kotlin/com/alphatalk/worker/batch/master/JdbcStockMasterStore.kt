package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisStockMaster
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component
import java.sql.Date
import java.sql.Types

@Component
class JdbcStockMasterStore(
    private val jdbc: JdbcTemplate,
) : StockMasterStore {
    override fun upsertAll(stocks: List<KisStockMaster>): Int {
        if (stocks.isEmpty()) return 0
        jdbc.batchUpdate(
            """
            INSERT INTO stock_master (code, name, market, sector_code, shares_outstanding, is_active, listed_at, updated_at)
            VALUES (?, ?, ?, ?, ?, true, ?, now())
            ON CONFLICT (code) DO UPDATE SET
                name = EXCLUDED.name,
                market = EXCLUDED.market,
                sector_code = EXCLUDED.sector_code,
                shares_outstanding = EXCLUDED.shares_outstanding,
                is_active = true,
                listed_at = EXCLUDED.listed_at,
                updated_at = now()
            """.trimIndent(),
            stocks,
            stocks.size,
        ) { ps, stock ->
            ps.setString(1, stock.code)
            ps.setString(2, stock.name)
            ps.setString(3, stock.market.name)
            ps.setString(4, stock.sectorCode)
            if (stock.sharesOutstanding == null) ps.setNull(5, Types.BIGINT) else ps.setLong(5, stock.sharesOutstanding!!)
            if (stock.listedAt == null) ps.setNull(6, Types.DATE) else ps.setDate(6, Date.valueOf(stock.listedAt))
        }
        return stocks.size
    }

    override fun deactivateMissing(activeCodes: Collection<String>): Int {
        if (activeCodes.isEmpty()) return 0
        return jdbc.update { connection ->
            connection.prepareStatement(
                "UPDATE stock_master SET is_active = false, updated_at = now() WHERE is_active = true AND NOT (code = ANY(?))",
            ).apply {
                setArray(1, connection.createArrayOf("text", activeCodes.toTypedArray()))
            }
        }
    }
}
