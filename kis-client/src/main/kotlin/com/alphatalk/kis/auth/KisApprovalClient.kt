package com.alphatalk.kis.auth

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.model.KisAccount
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse

class KisApprovalClient(
    private val restBaseUrl: String,
    private val http: HttpClient = HttpClient.newHttpClient(),
) {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    fun approvalKey(account: KisAccount): String {
        val body = mapper.writeValueAsString(
            mapOf(
                "grant_type" to "client_credentials",
                "appkey" to account.appkey,
                "secretkey" to account.appsecret,
            ),
        )
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$restBaseUrl/oauth2/Approval"))
            .header("content-type", "application/json; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw KisClientException("approval issue failed: keyId=${account.keyId} status=${response.statusCode()}")
        }
        val approvalKey = mapper.readTree(response.body()).path("approval_key").asText("")
        if (approvalKey.isEmpty()) {
            throw KisClientException("approval response invalid: keyId=${account.keyId}")
        }
        return approvalKey
    }
}
