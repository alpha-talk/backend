package com.alphatalk.worker.batch.opinion

import jakarta.persistence.Column
import jakarta.persistence.Embeddable
import jakarta.persistence.EmbeddedId
import jakarta.persistence.Entity
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.data.domain.Limit
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.time.Instant

@Embeddable
data class InvestOpinionKey(
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", nullable = false, length = 6)
    val code: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "business_date", nullable = false, length = 8)
    val businessDate: String = "",
    @Column(name = "broker_code", nullable = false)
    val brokerCode: String = "",
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "content_hash", nullable = false, length = 64)
    val contentHash: String = "",
) : Serializable

@Entity
@Table(name = "invest_opinion")
class InvestOpinionEntity(
    @EmbeddedId
    val key: InvestOpinionKey = InvestOpinionKey(),
    @Column(name = "broker_name")
    val brokerName: String? = null,
    @Column(name = "rating", nullable = false)
    val rating: String = "",
    @Column(name = "previous_rating")
    val previousRating: String? = null,
    @Column(name = "target_price")
    val targetPrice: Long? = null,
    @Column(name = "collected_at", nullable = false)
    val collectedAt: Instant = Instant.EPOCH,
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "stream_event_id", length = 26)
    var streamEventId: String? = null,
    @Column(name = "published_at")
    var publishedAt: Instant? = null,
)

interface InvestOpinionJpaRepository : JpaRepository<InvestOpinionEntity, InvestOpinionKey> {
    fun findByPublishedAtIsNullOrderByCollectedAt(limit: Limit): List<InvestOpinionEntity>

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update InvestOpinionEntity o
        set o.publishedAt = :at
        where o.streamEventId = :streamEventId and o.publishedAt is null
        """,
    )
    fun markPublished(@Param("streamEventId") streamEventId: String, @Param("at") at: Instant): Int

    @Transactional
    @Modifying(flushAutomatically = true, clearAutomatically = true)
    @Query(
        """
        update InvestOpinionEntity o
        set o.streamEventId = :streamEventId
        where o.key = :key
        """,
    )
    fun linkEvent(@Param("key") key: InvestOpinionKey, @Param("streamEventId") streamEventId: String): Int
}

@Repository
class JpaInvestOpinionStore(
    private val repository: InvestOpinionJpaRepository,
) : InvestOpinionStore {

    override fun insertIfAbsent(observation: OpinionObservation): Boolean {
        val key = observation.key()
        if (repository.existsById(key)) return false
        return try {
            repository.save(
                InvestOpinionEntity(
                    key = key,
                    brokerName = observation.brokerName,
                    rating = observation.rating,
                    previousRating = observation.previousRating,
                    targetPrice = observation.targetPrice,
                    collectedAt = observation.collectedAt,
                ),
            )
            true
        } catch (e: DataIntegrityViolationException) {
            if (repository.existsById(key)) false else throw e
        }
    }

    override fun findUnpublished(limit: Int): List<UnpublishedOpinion> =
        repository.findByPublishedAtIsNullOrderByCollectedAt(Limit.of(limit)).map { entity ->
            UnpublishedOpinion(
                observation = OpinionObservation(
                    code = entity.key.code,
                    businessDate = entity.key.businessDate,
                    brokerCode = entity.key.brokerCode,
                    brokerName = entity.brokerName,
                    rating = entity.rating,
                    previousRating = entity.previousRating,
                    targetPrice = entity.targetPrice,
                    contentHash = entity.key.contentHash,
                    collectedAt = entity.collectedAt,
                ),
                streamEventId = entity.streamEventId,
            )
        }

    override fun markPublished(streamEventId: String, at: Instant): Boolean =
        repository.markPublished(streamEventId, at) > 0
}

internal fun OpinionObservation.key(): InvestOpinionKey =
    InvestOpinionKey(code = code, businessDate = businessDate, brokerCode = brokerCode, contentHash = contentHash)
