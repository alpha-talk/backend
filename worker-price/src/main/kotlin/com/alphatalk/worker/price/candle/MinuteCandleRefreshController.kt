package com.alphatalk.worker.price.candle

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@ConditionalOnProperty(
    name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
    havingValue = "true",
)
class MinuteCandleRefreshController(
    private val service: MinuteCandleRefreshService,
) {
    @PostMapping("/internal/minute-candles/{code}/refresh")
    fun refresh(@PathVariable code: String): Map<String, Int> {
        require(code.length == 6 && code.all(Char::isDigit)) { "잘못된 종목코드: $code" }
        return mapOf("synced" to service.refresh(code))
    }
}
