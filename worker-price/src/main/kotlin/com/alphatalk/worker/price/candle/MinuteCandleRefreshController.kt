package com.alphatalk.worker.price.candle

import com.alphatalk.worker.price.config.ConditionalOnKisAccounts
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@ConditionalOnKisAccounts
class MinuteCandleRefreshController(
    private val service: MinuteCandleRefreshService,
    private val backfill: MinuteCandleBackfillService,
    private val universe: MinuteRefreshUniverse,
) {
    @PostMapping("/internal/minute-candles/{code}/refresh")
    fun refresh(@PathVariable code: String): Map<String, Int> {
        if (!CODE_PATTERN.matches(code)) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "종목 코드는 6자리 숫자여야 한다")
        }
        if (!universe.contains(code)) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "상장 종목이 아니다")
        }
        backfill.requestAsync(code)
        return mapOf("synced" to service.refresh(code))
    }

    private companion object {
        val CODE_PATTERN = Regex("^\\d{6}$")
    }
}
