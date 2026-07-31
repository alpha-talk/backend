package com.alphatalk.coreapi.search

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository

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
