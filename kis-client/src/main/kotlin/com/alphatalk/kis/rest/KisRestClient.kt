package com.alphatalk.kis.rest

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.rate.KisRateLimiters
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets

class KisRestClient(
    private val restBaseUrl: String,
    private val tokens: KisTokenManager,
    private val limiters: KisRateLimiters,
    private val http: HttpClient = HttpClient.newHttpClient(),
) {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    internal fun getJson(account: KisAccount, path: String, trId: String, params: Map<String, String>): JsonNode {
        val first = send(account, path, trId, params)
        if (first.statusCode() == 401) {
            tokens.invalidate(account.keyId)
            return parse(send(account, path, trId, params), account, path)
        }
        return parse(first, account, path)
    }

    private fun send(account: KisAccount, path: String, trId: String, params: Map<String, String>): HttpResponse<String> {
        limiters.acquire(account.keyId)
        val query = params.entries.joinToString("&") { "${encode(it.key)}=${encode(it.value)}" }
        val uri = URI.create(restBaseUrl + path + if (query.isEmpty()) "" else "?$query")
        val request = HttpRequest.newBuilder()
            .uri(uri)
            .header("content-type", "application/json; charset=utf-8")
            .header("authorization", "Bearer ${tokens.accessToken(account)}")
            .header("appkey", account.appkey)
            .header("appsecret", account.appsecret)
            .header("tr_id", trId)
            .header("custtype", "P")
            .GET()
            .build()
        return http.send(request, HttpResponse.BodyHandlers.ofString())
    }

    private fun parse(response: HttpResponse<String>, account: KisAccount, path: String): JsonNode {
        if (response.statusCode() != 200) {
            throw KisClientException(
                "rest call failed: keyId=${account.keyId} path=$path status=${response.statusCode()}",
            )
        }
        return mapper.readTree(response.body())
    }

    private fun encode(value: String): String = URLEncoder.encode(value, StandardCharsets.UTF_8)
}
