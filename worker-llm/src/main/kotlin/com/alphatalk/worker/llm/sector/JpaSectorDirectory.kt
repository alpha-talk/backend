package com.alphatalk.worker.llm.sector

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Entity
@Table(name = "sector")
class SectorEntity(
    @Id
    @Column(name = "code", nullable = false)
    var code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
)

@Entity
@Table(name = "stock_master")
class StockMasterEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    var code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "sector_code")
    var sectorCode: String? = null,
    @Column(name = "is_active", nullable = false)
    var isActive: Boolean = true,
)

interface SectorJpaRepository : JpaRepository<SectorEntity, String> {
    fun findAllByOrderByCodeAsc(): List<SectorEntity>
}

interface StockMasterJpaRepository : JpaRepository<StockMasterEntity, String> {
    fun findByIsActiveTrueAndSectorCodeOrderByCodeAsc(sectorCode: String): List<StockMasterEntity>
}

@Repository
class JpaSectorDirectory(
    private val sectors: SectorJpaRepository,
    private val stocks: StockMasterJpaRepository,
) : SectorDirectory {

    override fun allSectors(): List<SectorInfo> =
        sectors.findAllByOrderByCodeAsc().map { SectorInfo(code = it.code, name = it.name) }

    override fun sectorName(sectorCode: String): String? =
        sectors.findById(sectorCode).orElse(null)?.name

    override fun memberCodes(sectorCode: String): List<String> =
        stocks.findByIsActiveTrueAndSectorCodeOrderByCodeAsc(sectorCode).map { it.code.trim() }

    override fun stockName(stockCode: String): String? =
        stocks.findById(stockCode).orElse(null)?.name

    override fun sectorOf(stockCode: String): String? =
        stocks.findById(stockCode).orElse(null)?.sectorCode
}
