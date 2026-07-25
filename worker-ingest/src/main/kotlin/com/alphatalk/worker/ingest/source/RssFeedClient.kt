package com.alphatalk.worker.ingest.source

fun interface RssFeedClient {
    fun fetch(request: RssFeedRequest): RssFeedResponse
}

data class RssFeedRequest(
    val url: String,
    val etag: String?,
    val lastModified: Long?,
)

sealed interface RssFeedResponse {
    data class Modified(
        val body: ByteArray,
        val etag: String?,
        val lastModified: Long?,
    ) : RssFeedResponse

    data object NotModified : RssFeedResponse
}
