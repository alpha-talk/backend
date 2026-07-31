package com.alphatalk.coreapi.subscription

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

@Entity
@Table(name = "watchlist_rev")
class WatchlistRevisionEntity(
    @Id
    @Column(name = "user_id")
    var userId: Long = 0,
    @Column(name = "rev", nullable = false)
    var rev: Long = 0,
)

interface WatchlistJpaRepository : JpaRepository<WatchlistEntity, WatchlistId> {
    fun findByUserIdOrderByCreatedAtDescCodeAsc(userId: Long): List<WatchlistEntity>

    fun existsByUserIdAndCode(userId: Long, code: String): Boolean

    fun countByUserId(userId: Long): Long

    fun deleteByUserIdAndCode(userId: Long, code: String): Long
}

interface WatchlistRevisionJpaRepository : JpaRepository<WatchlistRevisionEntity, Long>

@Repository
class JpaWatchlistStore(
    private val watchlist: WatchlistJpaRepository,
    private val revisions: WatchlistRevisionJpaRepository,
    private val stocks: StockCatalog,
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

    override fun contains(userId: Long, code: String): Boolean =
        watchlist.existsByUserIdAndCode(userId, code)

    override fun count(userId: Long): Long = watchlist.countByUserId(userId)

    override fun add(userId: Long, code: String) {
        watchlist.save(WatchlistEntity(userId = userId, code = code))
    }

    override fun remove(userId: Long, code: String): Boolean =
        watchlist.deleteByUserIdAndCode(userId, code) > 0

    override fun codes(userId: Long): List<String> =
        watchlist.findByUserIdOrderByCreatedAtDescCodeAsc(userId).map { it.code.trim() }

    override fun nextRev(userId: Long): Long {
        val revision = revisions.findById(userId).orElseGet { WatchlistRevisionEntity(userId = userId, rev = 0) }
        revision.rev += 1
        revisions.save(revision)
        return revision.rev
    }
}
