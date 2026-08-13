package com.alphatalk.coreapi.notification

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.IdClass
import jakarta.persistence.Table
import org.hibernate.annotations.Immutable
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Sort
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.io.Serializable
import java.time.Instant

data class NotificationOpinionId(
    val code: String = "",
    val businessDate: String = "",
    val brokerCode: String = "",
    val contentHash: String = "",
) : Serializable

@Entity(name = "NotificationOpinion")
@Immutable
@Table(name = "invest_opinion")
@IdClass(NotificationOpinionId::class)
class NotificationOpinionEntity(
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "code", length = 6, columnDefinition = "char(6)")
    val code: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "business_date", length = 8, columnDefinition = "char(8)")
    val businessDate: String = "",
    @Id
    @Column(name = "broker_code")
    val brokerCode: String = "",
    @Id
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "content_hash", length = 64, columnDefinition = "char(64)")
    val contentHash: String = "",
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
    @Column(name = "stream_event_id", length = 26, columnDefinition = "char(26)")
    val streamEventId: String? = null,
)

interface OpinionEventIdView {
    val streamEventId: String?
}

interface NotificationOpinionJpaRepository : JpaRepository<NotificationOpinionEntity, NotificationOpinionId> {
    fun findByStreamEventIdIsNotNull(pageable: PageRequest): List<NotificationOpinionEntity>

    fun findByStreamEventIdGreaterThan(eventId: String, pageable: PageRequest): List<NotificationOpinionEntity>

    fun findEventIdsByStreamEventIdIsNotNull(pageable: PageRequest): List<OpinionEventIdView>

    fun findEventIdsByStreamEventIdGreaterThan(eventId: String, pageable: PageRequest): List<OpinionEventIdView>

    fun findByStreamEventIdLessThan(eventId: String, pageable: PageRequest): List<NotificationOpinionEntity>

    fun existsByStreamEventIdGreaterThan(eventId: String): Boolean

    fun existsByStreamEventIdLessThan(eventId: String): Boolean

    fun findTopByStreamEventIdIsNotNullOrderByStreamEventIdDesc(): NotificationOpinionEntity?
}

@Repository
@Transactional(readOnly = true)
class JpaOpinionFeed(
    private val opinions: NotificationOpinionJpaRepository,
) : OpinionFeed {
    override fun countNewerThan(afterEventId: String?, fetchLimit: Int): Int {
        val page = latestFirst(fetchLimit)
        val unread = afterEventId
            ?.let { opinions.findEventIdsByStreamEventIdGreaterThan(it, page) }
            ?: opinions.findEventIdsByStreamEventIdIsNotNull(page)
        return unread.size
    }

    override fun findLatest(beforeEventId: String?, limit: Int): List<OpinionRecord> {
        val page = latestFirst(limit)
        val rows = beforeEventId
            ?.let { opinions.findByStreamEventIdLessThan(it, page) }
            ?: opinions.findByStreamEventIdIsNotNull(page)
        return rows.map(::toRecord)
    }

    override fun hasOlderThan(eventId: String): Boolean = opinions.existsByStreamEventIdLessThan(eventId)

    override fun hasNewerThan(eventId: String): Boolean = opinions.existsByStreamEventIdGreaterThan(eventId)

    override fun latestEventId(): String? =
        opinions.findTopByStreamEventIdIsNotNullOrderByStreamEventIdDesc()?.streamEventId?.trim()

    private fun latestFirst(limit: Int) = PageRequest.of(0, limit, Sort.by(Sort.Direction.DESC, "streamEventId"))

    private fun toRecord(entity: NotificationOpinionEntity) = OpinionRecord(
        eventId = checkNotNull(entity.streamEventId).trim(),
        code = entity.code.trim(),
        businessDate = entity.businessDate.trim(),
        brokerCode = entity.brokerCode,
        brokerName = entity.brokerName,
        rating = entity.rating,
        previousRating = entity.previousRating,
        targetPrice = entity.targetPrice,
        collectedAt = entity.collectedAt,
    )
}
