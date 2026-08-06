package com.alphatalk.worker.llm.sector

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
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
    @Query(
        """
        select s from SectorEntity s
        where s.code in (
            select distinct m.sectorCode from StockMasterEntity m
            where m.isActive = true and m.sectorCode is not null
        )
        order by s.code asc
        """,
    )
    fun findAllInUse(): List<SectorEntity>
}

interface StockMasterJpaRepository : JpaRepository<StockMasterEntity, String> {
    @Query(
        """
        select trim(m.code) from StockMasterEntity m
        where m.isActive = true and m.sectorCode = :sectorCode
        order by m.code asc
        """,
    )
    fun findActiveMemberCodes(@Param("sectorCode") sectorCode: String): List<String>

    fun findByIsActiveTrue(): List<StockMasterEntity>
}

@Repository
class JpaSectorDirectory(
    private val sectors: SectorJpaRepository,
    private val stocks: StockMasterJpaRepository,
) : SectorDirectory {

    override fun allSectors(): List<SectorInfo> =
        sectors.findAllInUse()
            .map { SectorInfo(code = it.code, name = it.name) }
            .also {
                check(it.isNotEmpty()) {
                    "업종 축이 비어 있다 — worker-batch industry_sync가 sector·stock_master.sector_code를 적재해야 한다"
                }
            }

    override fun sectorName(sectorCode: String): String? =
        sectors.findById(sectorCode).orElse(null)?.name

    override fun memberCodes(sectorCode: String): List<String> =
        stocks.findActiveMemberCodes(sectorCode)

    override fun stockName(stockCode: String): String? =
        stocks.findById(stockCode).orElse(null)?.takeIf { it.isActive }?.name

    override fun sectorOf(stockCode: String): String? =
        stocks.findById(stockCode).orElse(null)?.sectorCode
}
