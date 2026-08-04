package com.alphatalk.coreapi.stockinfo

import org.slf4j.LoggerFactory
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient
import java.time.Duration

class HttpMinuteCandleRefresher(
    baseUrl: String,
    connectTimeout: Duration = Duration.ofMillis(500),
    readTimeout: Duration = Duration.ofMillis(1_000),
) : MinuteCandleRefresher {
    private val log = LoggerFactory.getLogger(javaClass)
    private val client: RestClient = RestClient.builder()
        .baseUrl(baseUrl)
        .requestFactory(
            SimpleClientHttpRequestFactory().apply {
                setConnectTimeout(connectTimeout)
                setReadTimeout(readTimeout)
            },
        )
        .build()

    override fun refresh(code: String) {
        runCatching {
            client.post()
                .uri("/internal/minute-candles/{code}/refresh", code)
                .retrieve()
                .toBodilessEntity()
        }.onFailure {
            log.warn("minute candle refresh trigger failed: code={} reason={}", code, it.message)
        }
    }
}
