package com.alphatalk.worker.ingest.source

interface NewsSource {
    val id: String
        get() = name
    val name: String
    val url: String
    fun fetchLatest(validators: FeedValidators): FeedUpdate?
}

data class FeedValidators(val etag: String? = null, val lastModified: Long? = null)

data class FeedUpdate(val articles: List<FetchedArticle>, val validators: FeedValidators = FeedValidators())

data class FetchedArticle(
    val sourceId: String? = null,
    val title: String,
    val url: String,
    val excerpt: String? = null,
    val publishedAt: Long? = null,
    val codes: List<String> = emptyList(),
)
