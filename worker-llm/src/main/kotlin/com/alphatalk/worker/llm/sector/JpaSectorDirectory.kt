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
@Table(name = "industry")
class IndustryEntity(
    @Id
    @Column(name = "code", nullable = false)
    var code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "level", nullable = false)
    var level: Short = 0,
)

@Entity
@Table(name = "stock_industry")
class StockIndustryEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    var code: String = "",
    @Column(name = "group_code", nullable = false)
    var groupCode: String = "",
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
    @Column(name = "is_active", nullable = false)
    var isActive: Boolean = true,
)

interface IndustryJpaRepository : JpaRepository<IndustryEntity, String> {
    @Query(
        """
        select i from IndustryEntity i
        where i.code in (
            select distinct si.groupCode from StockIndustryEntity si
            where exists (
                select 1 from StockMasterEntity m where m.code = si.code and m.isActive = true
            )
        )
        order by i.code asc
        """,
    )
    fun findAllInUse(): List<IndustryEntity>
}

interface StockIndustryJpaRepository : JpaRepository<StockIndustryEntity, String> {
    @Query(
        """
        select trim(si.code) from StockIndustryEntity si
        where si.groupCode = :groupCode
          and exists (select 1 from StockMasterEntity m where m.code = si.code and m.isActive = true)
        order by si.code asc
        """,
    )
    fun findActiveMemberCodes(@Param("groupCode") groupCode: String): List<String>
}

interface StockNameJpaRepository : JpaRepository<StockMasterEntity, String>

@Repository
class JpaSectorDirectory(
    private val industries: IndustryJpaRepository,
    private val stockIndustries: StockIndustryJpaRepository,
    private val stocks: StockNameJpaRepository,
) : SectorDirectory {

    override fun allSectors(): List<SectorInfo> =
        industries.findAllInUse()
            .map { SectorInfo(code = it.code, name = it.name) }
            .also {
                check(it.isNotEmpty()) {
                    "업종 축이 비어 있다 — worker-batch industry_sync가 industry·stock_industry를 적재해야 한다"
                }
            }

    override fun sectorName(sectorCode: String): String? =
        industries.findById(sectorCode).orElse(null)?.name

    override fun memberCodes(sectorCode: String): List<String> =
        stockIndustries.findActiveMemberCodes(sectorCode)

    override fun stockName(stockCode: String): String? =
        stocks.findById(stockCode).orElse(null)?.takeIf { it.isActive }?.name

    override fun sectorOf(stockCode: String): String? =
        stockIndustries.findById(stockCode).orElse(null)?.groupCode
}
