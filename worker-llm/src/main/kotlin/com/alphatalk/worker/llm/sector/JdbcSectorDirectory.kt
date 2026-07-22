package com.alphatalk.worker.llm.sector

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate

class JdbcSectorDirectory(
    private val jdbc: NamedParameterJdbcTemplate,
) : SectorDirectory {

    override fun allSectors(): List<SectorInfo> =
        jdbc.query("SELECT code, name FROM sector ORDER BY code", emptyMap<String, Any>()) { rs, _ ->
            SectorInfo(code = rs.getString("code"), name = rs.getString("name"))
        }

    override fun sectorName(sectorCode: String): String? =
        jdbc.query(
            "SELECT name FROM sector WHERE code = :code",
            mapOf("code" to sectorCode),
        ) { rs, _ -> rs.getString(1) }.firstOrNull()

    override fun memberCodes(sectorCode: String): List<String> =
        jdbc.query(
            "SELECT code FROM stock_master WHERE sector_code = :sectorCode AND is_active ORDER BY code",
            mapOf("sectorCode" to sectorCode),
        ) { rs, _ -> rs.getString(1).trim() }

    override fun stockName(stockCode: String): String? =
        jdbc.query(
            "SELECT name FROM stock_master WHERE code = :code",
            mapOf("code" to stockCode),
        ) { rs, _ -> rs.getString(1) }.firstOrNull()

    override fun sectorOf(stockCode: String): String? =
        jdbc.query(
            "SELECT sector_code FROM stock_master WHERE code = :code",
            mapOf("code" to stockCode),
        ) { rs, _ -> rs.getString(1) }.firstOrNull()
}
