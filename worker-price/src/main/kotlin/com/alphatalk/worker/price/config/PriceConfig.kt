package com.alphatalk.worker.price.config

import com.alphatalk.kis.auth.KisApprovalClient
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisEnv
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.session.FixedSubscriptionRunner
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class PriceConfig {
    @Bean
    fun conflationBuffer(): ConflationBuffer = ConflationBuffer()

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun fixedSubscriptionRunner(
        props: PriceProperties,
        buffer: ConflationBuffer,
        meters: MeterRegistry,
    ): FixedSubscriptionRunner {
        val env = KisEnv.valueOf(props.env.trim().uppercase())
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        check(props.symbols.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 symbols가 최소 1개 필요하다"
        }
        check(props.symbols.size <= KisLimits.MAX_SYMBOLS_PER_SESSION) {
            "고정 종목은 세션당 등록 한도 ${KisLimits.MAX_SYMBOLS_PER_SESSION}건을 넘을 수 없다"
        }
        val account = accounts.first()
        val approvals = KisApprovalClient(env.restBaseUrl)
        return FixedSubscriptionRunner(
            wsUrl = env.wsUrl,
            symbols = props.symbols,
            approvalKey = { approvals.approvalKey(account) },
            buffer = buffer,
            meters = meters,
        )
    }

    private fun parseAccounts(accountsJson: String): List<KisAccount> = try {
        jacksonObjectMapper().readValue(accountsJson)
    } catch (e: Exception) {
        throw IllegalStateException(
            "KIS_ACCOUNTS JSON 파싱 실패 — [{\"keyId\",\"appkey\",\"appsecret\"}] 배열이어야 한다",
        )
    }
}
