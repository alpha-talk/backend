package com.alphatalk.worker.llm.cluster

import com.alphatalk.worker.llm.config.LlmProperties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.http.MediaType
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient

class RestEmbeddingClient(
    private val props: LlmProperties.Embedding,
    private val rest: RestClient = RestClient.builder()
        .baseUrl(props.baseUrl)
        .requestFactory(
            SimpleClientHttpRequestFactory().apply {
                setConnectTimeout(props.connectTimeout)
                setReadTimeout(props.readTimeout)
            },
        )
        .build(),
) : EmbeddingClient {

    private val mapper = jacksonObjectMapper()

    override val dimension: Int = props.dimension

    override fun embed(text: String): FloatArray {
        val body = mapOf("model" to props.model, "input" to text, "dimensions" to props.dimension)
        val response = rest.post()
            .uri("/v1/embeddings")
            .header("Authorization", "Bearer ${props.apiKey}")
            .contentType(MediaType.APPLICATION_JSON)
            .body(mapper.writeValueAsString(body))
            .retrieve()
            .body(String::class.java)
            ?: throw IllegalStateException("empty embedding response")
        val values = mapper.readTree(response).path("data").path(0).path("embedding")
        require(values.isArray && values.size() == props.dimension) {
            "unexpected embedding shape: ${values.size()}"
        }
        return FloatArray(values.size()) { values[it].floatValue() }
    }
}
