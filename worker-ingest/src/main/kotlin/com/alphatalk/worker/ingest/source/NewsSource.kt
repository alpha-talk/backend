package com.alphatalk.worker.ingest.source

interface NewsSource {
    val name: String
    fun fetchLatest(): List<FetchedArticle>
}

data class FetchedArticle(
    val sourceId: String? = null,
    val title: String,
    val url: String,
    val excerpt: String? = null,
    val publishedAt: Long? = null,
)
