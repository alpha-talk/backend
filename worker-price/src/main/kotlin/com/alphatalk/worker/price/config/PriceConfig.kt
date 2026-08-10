package com.alphatalk.worker.price.config

import com.alphatalk.kis.auth.KisApprovalClient
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.auth.KisTokenStore
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisApi
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.kis.ws.KisFrameParser
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
import com.alphatalk.worker.price.candle.RedisMinuteBackfillLock
import com.alphatalk.worker.price.candle.RedisMinuteMarketDivStore
import com.alphatalk.worker.price.candle.RedisMinuteRefreshLock
import com.alphatalk.worker.price.candle.RedisMinuteRefreshWatermarkStore
import com.alphatalk.worker.price.candle.StockMasterCodeRepository
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.demand.RedisDemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.leader.RedisLeaderLock
import com.alphatalk.worker.price.market.MarketDivStore
import com.alphatalk.worker.price.market.RedisMarketDivStore
import com.alphatalk.worker.price.poll.QuoteSnapshotFetcher
import com.alphatalk.worker.price.poll.RestPollingScheduler
import com.alphatalk.worker.price.poll.WarmupPoller
import com.alphatalk.worker.price.publish.QuotePublisher
import com.alphatalk.worker.price.rate.RedisKisRateGate
import com.alphatalk.worker.price.session.PriceLifecycle
import com.alphatalk.worker.price.session.PriceOrchestrator
import com.alphatalk.worker.price.session.SessionPool
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import java.lang.management.ManagementFactory
import kotlin.math.ceil
import java.time.Duration
import java.time.LocalDate

@Configuration
class PriceConfig {
    @Bean
    fun conflationBuffer(): ConflationBuffer = ConflationBuffer()

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun demandSource(
        props: PriceProperties,
        redis: StringRedisTemplate,
        connectionFactory: RedisConnectionFactory,
    ): DemandSource = RedisDemandSource(redis, connectionFactory, props.demandReconcileMs)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun sessionPool(
        props: PriceProperties,
        buffer: ConflationBuffer,
        meters: MeterRegistry,
        marketDivs: MarketDivStore,
    ): SessionPool {
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val approvals = KisApprovalClient(KisApi.REST_BASE_URL)
        return SessionPool(
            accounts = accounts,
            wsUrl = KisApi.WS_URL,
            approvalKeys = { approvals.approvalKey(it) },
            buffer = buffer,
            meters = meters,
            tickTrIds = TICK_TR_IDS,
            marketDivs = marketDivs,
            unifiedTrId = KisFrameParser.TR_ID_TICK_TOTAL,
            krxTrId = KisFrameParser.TR_ID_TICK,
            silenceMillis = props.tickSilenceMs,
            removalGraceMillis = props.removalGraceMs,
        )
    }

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun marketDivStore(redis: StringRedisTemplate): MarketDivStore = RedisMarketDivStore(redis)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun marketCalendar(props: PriceProperties): MarketCalendar = MarketCalendar(
        holidays = props.holidays.map(LocalDate::parse).toSet(),
        enforced = props.marketHoursEnforced,
    )

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun leaderLock(redis: StringRedisTemplate): LeaderLock =
        RedisLeaderLock(redis, instanceId = ManagementFactory.getRuntimeMXBean().name)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun priceOrchestrator(
        demand: DemandSource,
        pool: SessionPool,
        calendar: MarketCalendar,
        leader: LeaderLock,
        meters: MeterRegistry,
    ): PriceOrchestrator = PriceOrchestrator(demand, pool, calendar, leader, meters)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun priceLifecycle(orchestrator: PriceOrchestrator, props: PriceProperties): PriceLifecycle =
        PriceLifecycle(orchestrator, props.maintainIntervalMs)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun kisTokenManager(store: KisTokenStore): KisTokenManager =
        KisTokenManager(KisApi.REST_BASE_URL, store)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun kisRateGate(props: PriceProperties, redis: StringRedisTemplate): KisRateGate {
        val rate = KisLimits.REST_CALLS_PER_SECOND * props.rateFactor
        return RedisKisRateGate(
            redis = redis,
            capacity = ceil(rate).toInt().coerceAtLeast(1),
            refillPerSecond = rate,
        )
    }

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun quoteSnapshotFetcher(
        props: PriceProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
    ): QuoteSnapshotFetcher {
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
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
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun restPollingScheduler(
        pool: SessionPool,
        fetcher: QuoteSnapshotFetcher,
        marketDivs: MarketDivStore,
        publisher: QuotePublisher,
        calendar: MarketCalendar,
        leader: LeaderLock,
        meters: MeterRegistry,
    ): RestPollingScheduler =
        RestPollingScheduler(pool::degradedSymbols, fetcher, marketDivs, publisher, calendar, leader, meters)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun warmupPoller(
        demand: DemandSource,
        poller: RestPollingScheduler,
        calendar: MarketCalendar,
        leader: LeaderLock,
    ): WarmupPoller = WarmupPoller(demand, poller, calendar, leader)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun candleRestLimiters(props: PriceProperties): KisRateLimiters =
        KisRateLimiters(
            KisLimits.REST_CALLS_PER_SECOND,
            props.rateFactor * (1 - props.pollBudgetFactor),
            CANDLE_ACQUIRE_TIMEOUT,
        )

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.candle-enabled"],
        havingValue = "true",
    )
    fun dailyCandleFetcher(
        props: PriceProperties,
        tokens: KisTokenManager,
        candleRestLimiters: KisRateLimiters,
        gate: KisRateGate,
    ): DailyCandleFetcher {
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.candle-enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(KisApi.REST_BASE_URL, tokens, candleRestLimiters, gate)
        return DailyCandleFetcher { code, from, to -> rest.dailyCandles(account, code, from, to) }
    }

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.candle-enabled"],
        havingValue = "true",
    )
    fun candleUniverse(
        demand: DemandSource,
        masterCodes: StockMasterCodeRepository,
    ): CandleUniverse = MasterCandleUniverse(
        activeCodes = { masterCodes.findActiveCodes() },
        fallback = { demand.targetSymbols() },
    )

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.candle-enabled"],
        havingValue = "true",
    )
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
    @ConditionalOnProperty(
        name = [
            "alphatalk.price.enabled",
            "alphatalk.price.candle-enabled",
            "alphatalk.price.candle-sync-on-startup",
        ],
        havingValue = "true",
    )
    fun candleStartupSync(job: CandleSyncJob): ApplicationRunner = ApplicationRunner { job.syncOnce() }

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteCandleFetcher(
        props: PriceProperties,
        tokens: KisTokenManager,
        candleRestLimiters: KisRateLimiters,
        gate: KisRateGate,
    ): MinuteCandleFetcher {
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.minute-candle-enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(KisApi.REST_BASE_URL, tokens, candleRestLimiters, gate)
        return MinuteCandleFetcher { code, to, marketDiv ->
            rest.minuteCandles(account, code, to, marketDiv)
        }
    }

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun dailyMinuteCandleFetcher(
        props: PriceProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
    ): DailyMinuteCandleFetcher {
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.minute-candle-enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
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
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteBackfillLock(redis: StringRedisTemplate): MinuteBackfillLock =
        RedisMinuteBackfillLock(redis, instanceId = ManagementFactory.getRuntimeMXBean().name)

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
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
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteMarketDivStore(redis: StringRedisTemplate): MinuteMarketDivStore =
        RedisMinuteMarketDivStore(redis)

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteRefreshLock(redis: StringRedisTemplate): MinuteRefreshLock =
        RedisMinuteRefreshLock(redis, instanceId = ManagementFactory.getRuntimeMXBean().name)

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteRefreshWatermarkStore(redis: StringRedisTemplate): MinuteRefreshWatermarkStore =
        RedisMinuteRefreshWatermarkStore(redis)

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
    fun minuteRefreshUniverse(masterCodes: StockMasterCodeRepository): MinuteRefreshUniverse =
        MinuteRefreshUniverse { code -> masterCodes.existsByCodeAndActiveIsTrue(code) }

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
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
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
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
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.minute-candle-enabled"],
        havingValue = "true",
    )
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
        val TICK_TR_IDS: List<String> =
            listOf(KisFrameParser.TR_ID_TICK_TOTAL, KisFrameParser.TR_ID_TICK_OVERTIME)
        val CANDLE_ACQUIRE_TIMEOUT: Duration = Duration.ofSeconds(10)
        const val BACKFILL_BUDGET_SHARE = 0.25
    }

    internal fun parseAccounts(accountsJson: String): List<KisAccount> = try {
        jacksonObjectMapper().readValue(accountsJson)
    } catch (e: Exception) {
        throw IllegalStateException(
            "KIS_ACCOUNTS JSON 파싱 실패 — [{\"keyId\",\"appkey\",\"appsecret\"}] 배열이어야 한다",
        )
    }
}
