package com.alphatalk.coreapi.subscription

import com.alphatalk.coreapi.auth.UserAccountLock
import com.alphatalk.coreapi.search.StockCatalog
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Generated
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.generator.EventType
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.slf4j.LoggerFactory
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
    @Generated(event = [EventType.INSERT])
    var createdAt: Instant? = null,
)

interface WatchlistJpaRepository : JpaRepository<WatchlistEntity, WatchlistId> {
    fun findByUserIdOrderByCreatedAtDescCodeAsc(userId: Long): List<WatchlistEntity>

    fun existsByUserIdAndCode(userId: Long, code: String): Boolean

    fun countByUserId(userId: Long): Long

    fun deleteByUserIdAndCode(userId: Long, code: String): Long
}

@Repository
class JpaWatchlistStore(
    private val watchlist: WatchlistJpaRepository,
    private val stocks: StockCatalog,
    private val owners: UserAccountLock,
) : WatchlistStore {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun list(userId: Long): List<WatchlistItem> {
        val rows = watchlist.findByUserIdOrderByCreatedAtDescCodeAsc(userId)
        if (rows.isEmpty()) return emptyList()
        val codes = rows.map { it.code.trim() }
        val refs = stocks.refs(codes)
        val missingCodes = codes.filterNot(refs::containsKey)
        if (missingCodes.isNotEmpty()) {
            log.warn("watchlist stock references missing: userId={} codes={}", userId, missingCodes)
        }
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
        if (!owners.acquire(userId)) return SubscribeOutcome.OWNER_MISSING
        if (watchlist.existsByUserIdAndCode(userId, code)) return SubscribeOutcome.ALREADY_SUBSCRIBED
        if (!stocks.existsActive(code)) return SubscribeOutcome.UNKNOWN_STOCK
        if (watchlist.countByUserId(userId) >= limit) return SubscribeOutcome.LIMIT_EXCEEDED
        watchlist.save(WatchlistEntity(userId = userId, code = code))
        return SubscribeOutcome.ADDED
    }

    @Transactional
    override fun unsubscribe(userId: Long, code: String): UnsubscribeOutcome {
        if (!owners.acquire(userId)) return UnsubscribeOutcome.OWNER_MISSING
        return if (watchlist.deleteByUserIdAndCode(userId, code) > 0) {
            UnsubscribeOutcome.REMOVED
        } else {
            UnsubscribeOutcome.ALREADY_REMOVED
        }
    }

    override fun contains(userId: Long, code: String): Boolean =
        watchlist.existsByUserIdAndCode(userId, code)
}
