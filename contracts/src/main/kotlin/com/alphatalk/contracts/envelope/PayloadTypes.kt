package com.alphatalk.contracts.envelope

data class QuoteData(
    val price: Long,
    val prevClose: Long,
    val change: Long,
    val changeRate: Double,
    val volume: Long,
    val open: Long,
    val high: Long,
    val low: Long,
)

data class StreamData(
    val category: String,
    val title: String,
    val summary: String? = null,
    val sourceUrl: String? = null,
    val occurredAt: Long,
)

data class PostData(
    val kind: String,
    val postId: String,
    val parentId: String? = null,
    val author: Author,
    val content: String,
    val createdAt: Long,
) {
    data class Author(val id: Long, val nickname: String)
}
