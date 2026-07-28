package com.alphatalk.kis.auth

import com.alphatalk.kis.KisClientException
import com.alphatalk.kis.model.KisAccount
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.Instant

class KisTokenManager(
    private val restBaseUrl: String,
    private val store: KisTokenStore,
    private val http: HttpClient = HttpClient.newHttpClient(),
    private val clock: () -> Instant = Instant::now,
    private val lockTtl: Duration = Duration.ofSeconds(5),
    private val issueTimeout: Duration = Duration.ofSeconds(3),
    private val lockWaitMillis: Long = 200,
    private val lockRetries: Int = 25,
    private val expiryMargin: Duration = Duration.ofMinutes(5),
    private val reissueInterval: Duration = Duration.ofMinutes(1),
) {
    private val mapper: ObjectMapper = jacksonObjectMapper()

    init {
        require(issueTimeout < lockTtl) {
            "issueTimeout($issueTimeout)은 lockTtl($lockTtl)보다 짧아야 발급이 락 유효기간 안에 끝난다"
        }
    }

    fun accessToken(account: KisAccount): String {
        store.get(account.keyId)?.let { return it }
        repeat(lockRetries) {
            val lockToken = store.tryLock(account.keyId, lockTtl)
            if (lockToken != null) {
                try {
                    store.get(account.keyId)?.let { return it }
                    return issue(account)
                } finally {
                    store.unlock(account.keyId, lockToken)
                }
            }
            Thread.sleep(lockWaitMillis)
            store.get(account.keyId)?.let { return it }
        }
        throw KisClientException("token lock not acquired: keyId=${account.keyId}")
    }

    fun invalidate(keyId: String) {
        store.evict(keyId)
        store.markIssued(keyId, Instant.EPOCH)
    }

    private fun issue(account: KisAccount): String {
        val startedAt = clock()
        val last = store.lastIssuedAt(account.keyId)
        if (last != null && Duration.between(last, startedAt) < reissueInterval) {
            throw KisClientException("token reissue throttled: keyId=${account.keyId}")
        }
        store.markIssued(account.keyId, startedAt)
        val body = mapper.writeValueAsString(
            mapOf(
                "grant_type" to "client_credentials",
                "appkey" to account.appkey,
                "appsecret" to account.appsecret,
            ),
        )
        val request = HttpRequest.newBuilder()
            .uri(URI.create("$restBaseUrl/oauth2/tokenP"))
            .header("content-type", "application/json; charset=utf-8")
            .timeout(issueTimeout)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() != 200) {
            throw KisClientException("token issue failed: keyId=${account.keyId} status=${response.statusCode()}")
        }
        val json = mapper.readTree(response.body())
        val token = json.path("access_token").asText("")
        val expiresIn = json.path("expires_in").asLong(0)
        if (token.isEmpty() || expiresIn <= 0) {
            throw KisClientException("token response invalid: keyId=${account.keyId}")
        }
        val ttl = Duration.ofSeconds(expiresIn) - expiryMargin
        store.put(account.keyId, token, if (ttl.isNegative || ttl.isZero) Duration.ofSeconds(expiresIn) else ttl)
        return token
    }
}
