package com.alphatalk.worker.ingest.scheduler

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Component
import java.io.Serializable

data class WatchlistCodeId(
    val userId: Long = 0,
    val code: String = "",
) : Serializable

@Entity
@Immutable
@Table(name = "watchlist")
@IdClass(WatchlistCodeId::class)
class WatchlistCodeEntity(
    @Id
    @Column(name = "user_id")
    val userId: Long = 0,
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
)

interface WatchlistCodeRepository : JpaRepository<WatchlistCodeEntity, WatchlistCodeId> {
    @Query("select distinct w.code from WatchlistCodeEntity w order by w.code")
    fun findDistinctCodes(): List<String>
}

@Component
class WatchlistDigestUniverse(
    private val repository: WatchlistCodeRepository,
) : DigestUniverse {
    override fun codes(): List<String> = repository.findDistinctCodes()
}
