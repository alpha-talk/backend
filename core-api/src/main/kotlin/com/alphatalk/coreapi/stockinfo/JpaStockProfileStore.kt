package com.alphatalk.coreapi.stockinfo

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import java.time.Instant
import java.time.LocalDate

@Entity(name = "StockProfile")
@Immutable
@Table(name = "stock_master")
class StockProfileEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "name", nullable = false)
    val name: String = "",
    @Column(name = "market", nullable = false)
    val market: String = "",
    @Column(name = "sector_code")
    val sectorCode: String? = null,
    @Column(name = "shares_outstanding")
    val sharesOutstanding: Long? = null,
    @Column(name = "is_active", nullable = false)
    val isActive: Boolean = true,
    @Column(name = "listed_at")
    val listedAt: LocalDate? = null,
    @Column(name = "updated_at", nullable = false)
    val updatedAt: Instant = Instant.EPOCH,
)

@Entity(name = "SectorName")
@Immutable
@Table(name = "sector")
class SectorNameEntity(
    @Id
    @Column(name = "code")
    val code: String = "",
    @Column(name = "name", nullable = false)
    val name: String = "",
)

data class StockProfileRow(
    val entity: StockProfileEntity,
    val sectorName: String?,
)

interface StockProfileJpaRepository : JpaRepository<StockProfileEntity, String> {
    @Query(
        """
        select new com.alphatalk.coreapi.stockinfo.StockProfileRow(s, g.name)
        from StockProfile s left join SectorName g on g.code = s.sectorCode
        where s.code = :code and s.isActive = true
        """,
    )
    fun findActiveWithSector(@Param("code") code: String): StockProfileRow?

    @Query("select count(s) > 0 from StockProfile s where s.code = :code and s.isActive = true")
    fun existsActive(@Param("code") code: String): Boolean
}

@Repository
class JpaStockProfileStore(
    private val profiles: StockProfileJpaRepository,
) : StockProfileStore {
    override fun findActive(code: String): StockProfile? =
        profiles.findActiveWithSector(code)?.let {
            StockProfile(
                code = it.entity.code.trim(),
                name = it.entity.name,
                market = it.entity.market,
                sectorName = it.sectorName,
                sharesOutstanding = it.entity.sharesOutstanding,
                listedAt = it.entity.listedAt,
                updatedAt = it.entity.updatedAt,
            )
        }

    override fun existsActive(code: String): Boolean = profiles.existsActive(code)
}
