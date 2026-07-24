package com.alphatalk.worker.llm.article

fun interface ArticleFetcher {
    fun fetchBody(url: String): String?
}
