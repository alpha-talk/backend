package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.search.StockCatalog
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.time.Instant

data class WatchlistId(
    val userId: Long = 0,
    val code: String = "",
) : Serializable

@Entity
@Table(name = "watchlist")
@IdClass(WatchlistId::class)
class WatchlistEntity(
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    var code: String = "",
    @Column(name = "created_at", insertable = false, updatable = false)
    var createdAt: Instant? = null,
)

interface WatchlistJpaRepository : JpaRepository<WatchlistEntity, WatchlistId> {
    fun findByUserIdOrderByCreatedAtDescCodeAsc(userId: Long): List<WatchlistEntity>

    fun existsByUserIdAndCode(userId: Long, code: String): Boolean

    fun countByUserId(userId: Long): Long

    fun deleteByUserIdAndCode(userId: Long, code: String): Long

    @Query(value = "select id from users where id = :userId for update", nativeQuery = true)
    fun lockOwner(@Param("userId") userId: Long): Long?
}

@Repository
class JpaWatchlistStore(
    private val watchlist: WatchlistJpaRepository,
    private val stocks: StockCatalog,
) : WatchlistStore {
    override fun list(userId: Long): List<WatchlistItem> {
        val rows = watchlist.findByUserIdOrderByCreatedAtDescCodeAsc(userId)
        if (rows.isEmpty()) return emptyList()
        val refs = stocks.refs(rows.map { it.code.trim() })
        return rows.mapNotNull { row ->
            val code = row.code.trim()
            refs[code]?.let {
                WatchlistItem(
                    code = code,
                    name = it.name,
                    market = it.market,
                    subscribedAt = requireNotNull(row.createdAt).toEpochMilli(),
                )
            }
        }
    }

    @Transactional
    override fun subscribe(userId: Long, code: String, limit: Int): SubscribeOutcome {
        watchlist.lockOwner(userId)
        if (watchlist.existsByUserIdAndCode(userId, code)) return SubscribeOutcome.ALREADY_SUBSCRIBED
        if (!stocks.existsActive(code)) return SubscribeOutcome.UNKNOWN_STOCK
        if (watchlist.countByUserId(userId) >= limit) return SubscribeOutcome.LIMIT_EXCEEDED
        watchlist.save(WatchlistEntity(userId = userId, code = code))
        return SubscribeOutcome.ADDED
    }

    @Transactional
    override fun unsubscribe(userId: Long, code: String): Boolean =
        watchlist.deleteByUserIdAndCode(userId, code) > 0
}
