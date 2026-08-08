package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.worker.batch.master.StockMasterEntity
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository

interface StockUniverseJpaRepository : JpaRepository<StockMasterEntity, String> {
    @Query(
        """
        select new com.alphatalk.worker.batch.stockinfo.ActiveStock(trim(s.code), s.sharesOutstanding)
        from StockMasterEntity s
        where s.isActive = true
        """,
    )
    fun findActiveStocks(): List<ActiveStock>
}

@Repository
class JpaStockUniverse(
    private val repository: StockUniverseJpaRepository,
) : StockUniverse {
    override fun activeStocks(): List<ActiveStock> = repository.findActiveStocks()

    override fun activeCodes(): Set<String> = repository.findActiveStocks().mapTo(mutableSetOf(), ActiveStock::code)
}
