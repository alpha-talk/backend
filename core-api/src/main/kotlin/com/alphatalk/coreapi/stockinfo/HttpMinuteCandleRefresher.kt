package com.alphatalk.coreapi.stockinfo

import org.slf4j.LoggerFactory
import org.springframework.http.client.SimpleClientHttpRequestFactory
import org.springframework.web.client.RestClient
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class HttpMinuteCandleRefresher(
    baseUrl: String,
    connectTimeout: Duration = Duration.ofMillis(500),
    readTimeout: Duration = Duration.ofMillis(1_000),
    private val joinTimeout: Duration = Duration.ofMillis(1_000),
    private val clock: () -> Long = System::currentTimeMillis,
    private val throttleMillis: Long = 1_000,
) : MinuteCandleRefresher {
    private val log = LoggerFactory.getLogger(javaClass)
    private val inFlight = ConcurrentHashMap<String, CompletableFuture<Unit>>()
    private val lastTriggeredAt = ConcurrentHashMap<String, Long>()
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
        val last = lastTriggeredAt[code]
        if (last != null && clock() - last < throttleMillis) return

        val mine = CompletableFuture<Unit>()
        val existing = inFlight.putIfAbsent(code, mine)
        if (existing != null) {
            runCatching { existing.get(joinTimeout.toMillis(), TimeUnit.MILLISECONDS) }
            return
        }
        try {
            trigger(code)
            lastTriggeredAt[code] = clock()
        } finally {
            mine.complete(Unit)
            inFlight.remove(code, mine)
        }
    }

    private fun trigger(code: String) {
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
