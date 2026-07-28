package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisSector
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Component

@Component
class JdbcSectorStore(
    private val jdbc: JdbcTemplate,
) : SectorStore {
    override fun upsertAll(sectors: List<KisSector>): Int {
        if (sectors.isEmpty()) return 0
        jdbc.batchUpdate(
            """
            INSERT INTO sector (code, name) VALUES (?, ?)
            ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name
            """.trimIndent(),
            sectors,
            sectors.size,
        ) { ps, sector ->
            ps.setString(1, sector.code)
            ps.setString(2, sector.name)
        }
        return sectors.size
    }
}
