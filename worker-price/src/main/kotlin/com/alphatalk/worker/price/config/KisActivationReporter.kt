package com.alphatalk.worker.price.config

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class KisActivationReporter(private val accounts: ObjectProvider<KisAccounts>) {
    private val log = LoggerFactory.getLogger(javaClass)

    @EventListener(ApplicationReadyEvent::class)
    fun report() {
        val active = accounts.ifAvailable
        if (active == null) {
            log.warn("KIS_ACCOUNTS 미설정 — KIS 수집 평면 전체 비활성(실시간·폴링·일봉·분봉). 운영이라면 시크릿 주입 누락을 의심하라")
        } else {
            log.info("KIS 수집 평면 활성 — 계정 {}개", active.values.size)
        }
    }
}
