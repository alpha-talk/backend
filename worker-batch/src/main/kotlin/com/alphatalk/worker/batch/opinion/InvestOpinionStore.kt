package com.alphatalk.worker.batch.opinion

import java.time.Instant

interface InvestOpinionStore {
    fun insertIfAbsent(observation: OpinionObservation): Boolean
    fun findUnpublished(limit: Int): List<UnpublishedOpinion>
    fun markPublished(streamEventId: String, at: Instant): Boolean
}

data class UnpublishedOpinion(
    val observation: OpinionObservation,
    val streamEventId: String?,
)

interface OpinionEventBinder {
    fun ensureEvent(opinion: UnpublishedOpinion): String
}
