package com.alphatalk.coreapi.stockinfo

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MinuteCandleRefreshConfig {
    private val log = LoggerFactory.getLogger(javaClass)

    @Bean
    @ConditionalOnProperty("alphatalk.stockinfo.minute-refresh-url")
    fun httpMinuteCandleRefresher(
        @Value("\${alphatalk.stockinfo.minute-refresh-url}") baseUrl: String,
    ): MinuteCandleRefresher = HttpMinuteCandleRefresher(baseUrl)

    @Bean
    @ConditionalOnMissingBean(MinuteCandleRefresher::class)
    fun disabledMinuteCandleRefresher(): MinuteCandleRefresher {
        log.warn("minute-refresh-url 미설정 — 분봉 조회는 저장분만 반환한다 (신선화 트리거 비활성)")
        return MinuteCandleRefresher { }
    }
}
