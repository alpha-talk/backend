package com.alphatalk.worker.llm.consume

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.transaction.annotation.Transactional

@Entity
@Table(name = "news_ingest_inbox")
class CapturedArticleReceipt(
    @Id val sourceId: String,
    @Column(length = 16) val outcome: String?,
)

interface CapturedArticleReceipts : JpaRepository<CapturedArticleReceipt, String> {
    @Query("select r.outcome from CapturedArticleReceipt r where r.sourceId = :sourceId")
    fun findOutcome(sourceId: String): String?

    @Transactional
    @Modifying
    @Query("update CapturedArticleReceipt r set r.outcome = :outcome where r.sourceId = :sourceId and r.outcome is null")
    fun resolve(sourceId: String, outcome: String): Int
}
