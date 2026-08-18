package com.alphatalk.worker.price.config

import com.alphatalk.kis.auth.KisApprovalClient
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.auth.KisTokenStore
import com.alphatalk.kis.model.KisApi
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.alphatalk.kis.redis.RedisKisRateGate
import com.alphatalk.kis.redis.RedisKisTokenStore
import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.candle.CandleSyncJob
import com.alphatalk.worker.price.candle.CandleUniverse
import com.alphatalk.worker.price.candle.DailyCandleFetcher
import com.alphatalk.worker.price.candle.DailyCandleStore
import com.alphatalk.worker.price.candle.DailyMinuteCandleFetcher
import com.alphatalk.worker.price.candle.MasterCandleUniverse
import com.alphatalk.worker.price.candle.MinuteBackfillLock
import com.alphatalk.worker.price.candle.MinuteCandleBackfillService
import com.alphatalk.worker.price.candle.MinuteCandleDailySyncJob
import com.alphatalk.worker.price.candle.MinuteCandleFetcher
import com.alphatalk.worker.price.candle.MinuteCandlePurgeJob
import com.alphatalk.worker.price.candle.MinuteCandleRefreshService
import com.alphatalk.worker.price.candle.MinuteCandleStore
import com.alphatalk.worker.price.candle.MinuteMarketDivStore
import com.alphatalk.worker.price.candle.MinuteRefreshLock
import com.alphatalk.worker.price.candle.MinuteRefreshUniverse
import com.alphatalk.worker.price.candle.MinuteRefreshWatermarkStore
import com.alphatalk.worker.price.candle.StockMasterCodeRepository
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.market.MarketDivStore
import com.alphatalk.worker.price.poll.QuoteSnapshotFetcher
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.math.ceil
import java.time.Duration
import java.time.LocalDate

@Configuration
@ConditionalOnKisAccounts
class PriceConfig {
    @Bean
    fun kisAccounts(props: PriceProperties): KisAccounts = KisAccounts.parse(props.accountsJson)

    @Bean
    fun kisApprovalClient(): KisApprovalClient = KisApprovalClient(KisApi.REST_BASE_URL)

    @Bean
    fun kisTokenStore(redis: StringRedisTemplate): KisTokenStore = RedisKisTokenStore(redis)

    @Bean
    fun kisTokenManager(store: KisTokenStore): KisTokenManager =
        KisTokenManager(KisApi.REST_BASE_URL, store)

    @Bean
    fun kisRateGate(props: PriceProperties, redis: StringRedisTemplate): KisRateGate {
        val rate = KisLimits.REST_CALLS_PER_SECOND * props.rateFactor
        return RedisKisRateGate(
            redis = redis,
            capacity = ceil(rate).toInt().coerceAtLeast(1),
            refillPerSecond = rate,
        )
    }

    @Bean
    fun marketCalendar(props: PriceProperties): MarketCalendar = MarketCalendar(
        holidays = props.holidays.map(LocalDate::parse).toSet(),
        enforced = props.marketHoursEnforced,
    )

    @Bean
    fun quoteSnapshotFetcher(
        accounts: KisAccounts,
        props: PriceProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
    ): QuoteSnapshotFetcher {
        val account = accounts.values.first()
        val rest = KisRestClient(
            KisApi.REST_BASE_URL,
            tokens,
            KisRateLimiters(
                KisLimits.REST_CALLS_PER_SECOND,
                props.rateFactor * props.pollBudgetFactor,
            ),
            gate,
        )
        return QuoteSnapshotFetcher { code, marketDiv -> rest.quoteSnapshot(account, code, marketDiv) }
    }

    @Bean
    fun candleRestLimiters(props: PriceProperties): KisRateLimiters =
        KisRateLimiters(
            KisLimits.REST_CALLS_PER_SECOND,
            props.rateFactor * (1 - props.pollBudgetFactor),
            CANDLE_ACQUIRE_TIMEOUT,
        )

    @Bean
    fun dailyCandleFetcher(
        accounts: KisAccounts,
        tokens: KisTokenManager,
        candleRestLimiters: KisRateLimiters,
        gate: KisRateGate,
    ): DailyCandleFetcher {
        val account = accounts.values.first()
        val rest = KisRestClient(KisApi.REST_BASE_URL, tokens, candleRestLimiters, gate)
        return DailyCandleFetcher { code, from, to -> rest.dailyCandles(account, code, from, to) }
    }

    @Bean
    fun candleUniverse(
        demand: DemandSource,
        masterCodes: StockMasterCodeRepository,
    ): CandleUniverse = MasterCandleUniverse(
        activeCodes = { masterCodes.findActiveCodes() },
        fallback = { demand.targetSymbols() },
    )

    @Bean
    fun candleSyncJob(
        universe: CandleUniverse,
        fetcher: DailyCandleFetcher,
        store: DailyCandleStore,
        calendar: MarketCalendar,
        leader: LeaderLock,
        meters: MeterRegistry,
        props: PriceProperties,
    ): CandleSyncJob = CandleSyncJob(
        symbols = { universe.symbols() },
        fetcher = fetcher,
        store = store,
        backfillDays = props.candleBackfillDays,
        calendar = calendar,
        leader = leader,
        meters = meters,
    )

    @Bean
    @ConditionalOnProperty("alphatalk.price.candle-sync-on-startup", havingValue = "true")
    fun candleStartupSync(job: CandleSyncJob): ApplicationRunner = ApplicationRunner { job.syncOnce() }

    @Bean
    fun minuteCandleFetcher(
        accounts: KisAccounts,
        tokens: KisTokenManager,
        candleRestLimiters: KisRateLimiters,
        gate: KisRateGate,
    ): MinuteCandleFetcher {
        val account = accounts.values.first()
        val rest = KisRestClient(KisApi.REST_BASE_URL, tokens, candleRestLimiters, gate)
        return MinuteCandleFetcher { code, to, marketDiv ->
            rest.minuteCandles(account, code, to, marketDiv)
        }
    }

    @Bean
    fun dailyMinuteCandleFetcher(
        accounts: KisAccounts,
        props: PriceProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
    ): DailyMinuteCandleFetcher {
        val account = accounts.values.first()
        val backfillLimiters = KisRateLimiters(
            KisLimits.REST_CALLS_PER_SECOND,
            props.rateFactor * (1 - props.pollBudgetFactor) * BACKFILL_BUDGET_SHARE,
            CANDLE_ACQUIRE_TIMEOUT,
        )
        val rest = KisRestClient(KisApi.REST_BASE_URL, tokens, backfillLimiters, gate)
        return DailyMinuteCandleFetcher { code, date, to, marketDiv ->
            rest.dailyMinuteCandles(account, code, date, to, marketDiv)
        }
    }

    @Bean
    fun minuteCandleBackfillService(
        fetcher: DailyMinuteCandleFetcher,
        store: MinuteCandleStore,
        calendar: MarketCalendar,
        backfillLock: MinuteBackfillLock,
        props: PriceProperties,
        meters: MeterRegistry,
    ): MinuteCandleBackfillService = MinuteCandleBackfillService(
        fetcher = fetcher,
        store = store,
        calendar = calendar,
        backfillLock = backfillLock,
        backfillDays = props.minuteCandleBackfillDays,
        meters = meters,
        cooldownMillis = props.minuteCandleBackfillCooldownSec * 1_000,
    )

    @Bean
    fun minuteRefreshUniverse(masterCodes: StockMasterCodeRepository): MinuteRefreshUniverse =
        MinuteRefreshUniverse { code -> masterCodes.existsByCodeAndActiveIsTrue(code) }

    @Bean
    fun minuteCandleRefreshService(
        fetcher: MinuteCandleFetcher,
        store: MinuteCandleStore,
        calendar: MarketCalendar,
        refreshLock: MinuteRefreshLock,
        watermarks: MinuteRefreshWatermarkStore,
        marketDivs: MinuteMarketDivStore,
        knownDivs: MarketDivStore,
        props: PriceProperties,
        meters: MeterRegistry,
    ): MinuteCandleRefreshService = MinuteCandleRefreshService(
        fetcher = fetcher,
        store = store,
        calendar = calendar,
        refreshLock = refreshLock,
        watermarks = watermarks,
        marketDivs = marketDivs,
        knownDivs = knownDivs,
        freshSeconds = props.minuteCandleFreshSec,
        meters = meters,
    )

    @Bean
    fun minuteCandleDailySyncJob(
        demand: DemandSource,
        store: MinuteCandleStore,
        service: MinuteCandleRefreshService,
        calendar: MarketCalendar,
        leader: LeaderLock,
    ): MinuteCandleDailySyncJob = MinuteCandleDailySyncJob(
        symbols = { demand.targetSymbols() },
        store = store,
        syncDay = service::syncDay,
        isDayComplete = service::isDayComplete,
        calendar = calendar,
        leader = leader,
    )

    @Bean
    fun minuteCandlePurgeJob(
        store: MinuteCandleStore,
        leader: LeaderLock,
        props: PriceProperties,
    ): MinuteCandlePurgeJob = MinuteCandlePurgeJob(
        store = store,
        leader = leader,
        retentionDays = props.minuteCandleRetentionDays,
    )

    internal companion object {
        val CANDLE_ACQUIRE_TIMEOUT: Duration = Duration.ofSeconds(10)
        const val BACKFILL_BUDGET_SHARE = 0.25
    }
}
