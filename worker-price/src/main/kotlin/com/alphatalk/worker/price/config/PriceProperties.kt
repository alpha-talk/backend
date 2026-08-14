package com.alphatalk.worker.price.config

import com.alphatalk.contracts.DemandTiming
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("alphatalk.price")
data class PriceProperties(
    val enabled: Boolean = false,
    val accountsJson: String = "[]",
    val conflationMs: Long = 200,
    val rateFactor: Double = 0.75,
    val maintainIntervalMs: Long = 1_000,
    val removalGraceMs: Long = 30_000,
    val demandReconcileMs: Long = 10_000,
    val marketHoursEnforced: Boolean = true,
    val holidays: List<String> = emptyList(),
    val pollIntervalMs: Long = 30_000,
    val tickSilenceMs: Long = 20_000,
    val pollBudgetFactor: Double = 0.5,
    val depthEnabled: Boolean = false,
    val candleEnabled: Boolean = false,
    val candleBackfillDays: Long = 90,
    val candleSyncOnStartup: Boolean = false,
    val minuteCandleEnabled: Boolean = false,
    val minuteCandleFreshSec: Long = 60,
    val minuteCandleRetentionDays: Long = 30,
    val minuteCandleBackfillDays: Int = 7,
    val minuteCandleBackfillCooldownSec: Long = 600,
) {
    init {
        val reregisterMs = DemandTiming.GATEWAY_HEARTBEAT_SECONDS * 1_000
        require(demandReconcileMs > 0 && removalGraceMs > reregisterMs && demandReconcileMs < removalGraceMs - reregisterMs) {
            "게이트웨이 재등록 지연(${reregisterMs}ms) + alphatalk.price.demand-reconcile-ms는 " +
                "removal-grace-ms보다 짧아야 수요 유실이 구독 해제 전에 복구된다: " +
                "demandReconcileMs=$demandReconcileMs removalGraceMs=$removalGraceMs"
        }
        require(conflationMs in MIN_CONFLATION_MS..MAX_CONFLATION_MS) {
            "alphatalk.price.conflation-ms는 ${MIN_CONFLATION_MS}~${MAX_CONFLATION_MS}ms여야 한다" +
                "(KIS 워커 명세 §2.4). 0에 가까우면 플러시가 사실상 바쁜 대기가 되어 Redis를 두드리고, " +
                "크면 틱이 그만큼 묵은 채로 배달된다: conflationMs=$conflationMs"
        }
    }

    companion object {
        const val MIN_CONFLATION_MS = 100L
        const val MAX_CONFLATION_MS = 250L
    }
}
