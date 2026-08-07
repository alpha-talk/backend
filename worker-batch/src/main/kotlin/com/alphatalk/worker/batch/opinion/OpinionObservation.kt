package com.alphatalk.worker.batch.opinion

import com.alphatalk.contracts.envelope.OpinionData
import com.alphatalk.contracts.envelope.StreamCategory
import com.alphatalk.contracts.envelope.StreamData
import java.security.MessageDigest
import java.time.Instant

data class OpinionObservation(
    val code: String,
    val businessDate: String,
    val brokerCode: String,
    val brokerName: String?,
    val rating: String,
    val previousRating: String?,
    val targetPrice: Long?,
    val contentHash: String,
    val collectedAt: Instant,
) {
    val sourceKey: String
        get() = "opinion:$code:$businessDate:$brokerCode:$contentHash"

    val title: String
        get() = "${brokerName ?: brokerCode} 투자의견 $rating"

    fun toStreamData(): StreamData = StreamData(
        category = StreamCategory.REPORT.payload,
        title = title,
        occurredAt = collectedAt.toEpochMilli(),
        kind = OpinionData.KIND,
        opinion = OpinionData(
            brokerCode = brokerCode,
            brokerName = brokerName,
            rating = rating,
            previousRating = previousRating,
            targetPrice = targetPrice,
            businessDate = businessDate,
        ),
    )

    companion object {
        fun contentHash(rating: String, previousRating: String?, targetPrice: Long?): String {
            val canonical = "${rating.trim()}|${previousRating?.trim().orEmpty()}|${targetPrice?.toString().orEmpty()}"
            val digest = MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray(Charsets.UTF_8))
            return digest.joinToString("") { "%02x".format(it) }
        }
    }
}
