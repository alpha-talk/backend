package com.alphatalk.coreapi.search

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable

@Entity
@Immutable
@Table(name = "stock_master")
class StockMasterEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @Column(name = "name", nullable = false)
    val name: String = "",
    @Column(name = "market", nullable = false)
    val market: String = "",
    @Column(name = "shares_outstanding")
    val sharesOutstanding: Long? = null,
    @Column(name = "is_active", nullable = false)
    val isActive: Boolean = true,
)

data class SearchValuationId(
    val code: String = "",
    val date: String = "",
) : Serializable

@Entity(name = "SearchValuation")
@Immutable
@Table(name = "valuation_daily")
@IdClass(SearchValuationId::class)
class SearchValuationEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "date", length = 8, columnDefinition = "char(8)")
    val date: String = "",
    @Column(name = "market_cap")
    val marketCap: Long? = null,
)

interface StockSearchJpaRepository : JpaRepository<StockMasterEntity, String> {
    @Query(
        """
        select new com.alphatalk.coreapi.search.StockSummary(trim(s.code), s.name, s.market)
        from StockMasterEntity s
        where s.isActive = true
          and (s.code like :prefix escape '!' or s.name ilike :contains escape '!')
        order by
            case
                when s.code like :prefix escape '!' then 0
                when s.name ilike :prefix escape '!' then 1
                else 2
            end,
            (
                select v.marketCap from SearchValuation v
                where v.code = s.code
                  and v.date = (select max(v2.date) from SearchValuation v2 where v2.code = s.code)
            ) desc nulls last,
            s.sharesOutstanding desc nulls last,
            s.code
        """,
    )
    fun search(@Param("prefix") prefix: String, @Param("contains") contains: String, pageable: Pageable): List<StockSummary>
}

@Repository
class JpaStockSearchStore(
    private val repository: StockSearchJpaRepository,
) : StockSearchStore {
    @Transactional(readOnly = true)
    override fun search(query: String, limit: Int): List<StockSummary> {
        val escaped = escapeLike(query)
        return repository.search("$escaped%", "%$escaped%", PageRequest.of(0, limit))
    }

    private fun escapeLike(value: String): String =
        value.replace("!", "!!").replace("%", "!%").replace("_", "!_")
}
