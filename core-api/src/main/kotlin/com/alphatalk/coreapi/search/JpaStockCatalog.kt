package com.alphatalk.coreapi.search

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

@Entity
@Immutable
@Table(name = "stock_master")
class StockMasterEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @Column(name = "name", nullable = false)
    var name: String = "",
    @Column(name = "market", nullable = false)
    var market: String = "",
    @Column(name = "is_active", nullable = false)
    var isActive: Boolean = true,
)

interface StockMasterJpaRepository : JpaRepository<StockMasterEntity, String> {
    fun existsByCodeAndIsActiveTrue(code: String): Boolean

    fun findByCodeIn(codes: Collection<String>): List<StockMasterEntity>
}

@Repository
class JpaStockCatalog(
    private val stocks: StockMasterJpaRepository,
) : StockCatalog {
    override fun existsActive(code: String): Boolean = stocks.existsByCodeAndIsActiveTrue(code)

    override fun refs(codes: Collection<String>): Map<String, StockRef> {
        if (codes.isEmpty()) return emptyMap()
        return stocks.findByCodeIn(codes)
            .associate { it.code.trim() to StockRef(it.code.trim(), it.name, it.market) }
    }
}
