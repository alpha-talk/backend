package com.alphatalk.worker.ingest.inbox

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.worker.ingest.source.FeedValidators
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.EntityManager
import jakarta.persistence.Id
import jakarta.persistence.Table
import jakarta.persistence.Version
import org.springframework.data.domain.Pageable
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import org.springframework.transaction.annotation.Transactional
import java.time.Instant

@Repository
class JpaIngestInbox(
    private val entityManager: EntityManager,
    private val articles: IngestInboxRepository,
    private val mapper: ObjectMapper,
) : IngestInbox {
    @Transactional(readOnly = true)
    override fun checkpoint(feedId: String, url: String): FeedCheckpoint? =
        entityManager.find(IngestFeedRow::class.java, feedId)?.let { row ->
            val validators = if (row.url == url) FeedValidators(row.etag, row.lastModified) else FeedValidators()
            FeedCheckpoint(row.version, validators)
        }

    @Transactional
    override fun capture(
        feedId: String,
        url: String,
        previous: FeedCheckpoint?,
        validators: FeedValidators,
        entries: List<IngestQueueEntry>,
    ): Int {
        val feed = entityManager.find(IngestFeedRow::class.java, feedId)
        if (feed == null) {
            entityManager.persist(IngestFeedRow(feedId, url, validators.etag, validators.lastModified))
        } else if (feed.version == previous?.version) {
            feed.url = url
            feed.etag = validators.etag
            feed.lastModified = validators.lastModified
        }

        val unique = entries.distinctBy(IngestQueueEntry::sourceId)
        val existing = articles.findAllById(unique.map(IngestQueueEntry::sourceId)).mapTo(hashSetOf()) { it.sourceId }
        val newEntries = unique.filterNot { it.sourceId in existing }
        val now = Instant.now()
        newEntries.forEach { entry ->
            entityManager.persist(IngestInboxRow(entry.sourceId, mapper.writeValueAsString(entry.toFields()), now))
        }
        return newEntries.size
    }

    fun decode(row: IngestInboxRow): IngestQueueEntry =
        IngestQueueEntry.fromFields(mapper.readValue(row.payload, object : TypeReference<Map<String, String>>() {}))
}

@Entity
@Table(name = "news_ingest_feed")
class IngestFeedRow(
    @Id val feedId: String,
    var url: String,
    var etag: String?,
    var lastModified: Long?,
    @Version var version: Long = 0,
)

@Entity
@Table(name = "news_ingest_inbox")
class IngestInboxRow(
    @Id val sourceId: String,
    val payload: String,
    val nextAttemptAt: Instant,
    @Column(length = 16) val outcome: String? = null,
)

interface IngestInboxRepository : JpaRepository<IngestInboxRow, String> {
    @Query("select i from IngestInboxRow i where i.outcome is null and i.nextAttemptAt <= :now order by i.nextAttemptAt, i.sourceId")
    fun findReady(now: Instant, pageable: Pageable): List<IngestInboxRow>

    @Transactional
    @Modifying
    @Query("update IngestInboxRow i set i.nextAttemptAt = :retryAt where i.sourceId = :sourceId and i.outcome is null and i.nextAttemptAt <= :now")
    fun deferIfReady(sourceId: String, now: Instant, retryAt: Instant): Int
}
