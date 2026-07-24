package com.alphatalk.worker.llm.cluster

interface EmbeddingClient {
    val dimension: Int
    fun embed(text: String): FloatArray
}
