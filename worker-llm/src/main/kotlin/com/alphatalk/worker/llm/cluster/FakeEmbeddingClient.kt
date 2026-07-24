package com.alphatalk.worker.llm.cluster

import kotlin.math.abs
import kotlin.math.sqrt

class FakeEmbeddingClient(override val dimension: Int) : EmbeddingClient {

    override fun embed(text: String): FloatArray {
        val vector = FloatArray(dimension)
        text.lowercase()
            .split(TOKEN_SEPARATOR)
            .filter { it.isNotBlank() }
            .forEach { token -> vector[abs(token.hashCode()) % dimension] += 1f }
        val norm = sqrt(vector.sumOf { (it * it).toDouble() }).toFloat()
        if (norm > 0f) {
            for (i in vector.indices) vector[i] /= norm
        }
        return vector
    }

    companion object {
        private val TOKEN_SEPARATOR = Regex("[\\s,.·'\"()\\[\\]]+")
    }
}
