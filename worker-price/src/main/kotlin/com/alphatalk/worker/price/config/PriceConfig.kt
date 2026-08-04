package com.alphatalk.worker.price.config

import com.alphatalk.kis.auth.KisApprovalClient
import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.auth.KisTokenStore
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisEnv
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.kis.ws.KisFrameParser
import com.alphatalk.worker.price.calendar.MarketCalendar
import com.alphatalk.worker.price.candle.CandleSyncJob
import com.alphatalk.worker.price.candle.CandleUniverse
import com.alphatalk.worker.price.candle.DailyCandleFetcher
import com.alphatalk.worker.price.candle.DailyCandleStore
import com.alphatalk.worker.price.candle.MasterCandleUniverse
import com.alphatalk.worker.price.candle.MinuteCandleDailySyncJob
import com.alphatalk.worker.price.candle.MinuteCandleFetcher
import com.alphatalk.worker.price.candle.MinuteCandlePurgeJob
import com.alphatalk.worker.price.candle.MinuteCandleRefreshService
import com.alphatalk.worker.price.candle.MinuteCandleStore
import com.alphatalk.worker.price.candle.MinuteRefreshLock
import com.alphatalk.worker.price.candle.RedisMinuteRefreshLock
import com.alphatalk.worker.price.candle.StockMasterCodeRepository
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.demand.DemandSource
import com.alphatalk.worker.price.demand.RedisDemandSource
import com.alphatalk.worker.price.leader.LeaderLock
import com.alphatalk.worker.price.leader.RedisLeaderLock
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
        redis: StringRedisTemplate,
        connectionFactory: RedisConnectionFactory,
    ): DemandSource = RedisDemandSource(redis, connectionFactory)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun sessionPool(
        props: PriceProperties,
        buffer: ConflationBuffer,
        meters: MeterRegistry,
    ): SessionPool {
        val env = kisEnv(props)
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val approvals = KisApprovalClient(env.restBaseUrl)
        return SessionPool(
            accounts = accounts,
            wsUrl = env.wsUrl,
            approvalKeys = { approvals.approvalKey(it) },
            buffer = buffer,
            meters = meters,
            tickTrIds = tickTrIds(env),
            removalGraceMillis = props.removalGraceMs,
        )
    }

    internal fun tickTrIds(env: KisEnv): List<String> = when (env) {
        KisEnv.PROD -> listOf(KisFrameParser.TR_ID_TICK_TOTAL, KisFrameParser.TR_ID_TICK_OVERTIME)
        KisEnv.VTS -> listOf(KisFrameParser.TR_ID_TICK)
    }

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
    fun kisTokenManager(props: PriceProperties, store: KisTokenStore): KisTokenManager =
        KisTokenManager(kisEnv(props).restBaseUrl, store)

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun kisRateGate(props: PriceProperties, redis: StringRedisTemplate): KisRateGate {
        val rate = kisEnv(props).restCallsPerSecond * props.rateFactor
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
        val env = kisEnv(props)
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(
            env.restBaseUrl,
            tokens,
            KisRateLimiters(env.restCallsPerSecond, props.rateFactor * props.pollBudgetFactor),
            gate,
        )
        return QuoteSnapshotFetcher { code -> rest.quoteSnapshot(account, code) }
    }

    @Bean
    @ConditionalOnProperty("alphatalk.price.enabled", havingValue = "true")
    fun restPollingScheduler(
        pool: SessionPool,
        fetcher: QuoteSnapshotFetcher,
        publisher: QuotePublisher,
        calendar: MarketCalendar,
        leader: LeaderLock,
        meters: MeterRegistry,
    ): RestPollingScheduler = RestPollingScheduler(pool::degradedSymbols, fetcher, publisher, calendar, leader, meters)

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
            kisEnv(props).restCallsPerSecond,
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
        val env = kisEnv(props)
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.candle-enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(env.restBaseUrl, tokens, candleRestLimiters, gate)
        return DailyCandleFetcher { code, from, to -> rest.dailyCandles(account, code, from, to) }
    }

    @Bean
    @ConditionalOnProperty(
        name = ["alphatalk.price.enabled", "alphatalk.price.candle-enabled"],
        havingValue = "true",
    )
    fun candleUniverse(
        props: PriceProperties,
        demand: DemandSource,
        masterCodes: StockMasterCodeRepository,
    ): CandleUniverse = when (kisEnv(props)) {
        KisEnv.PROD -> MasterCandleUniverse(
            activeCodes = { masterCodes.findActiveCodes() },
            fallback = { demand.targetSymbols() },
        )
        KisEnv.VTS -> CandleUniverse { demand.targetSymbols() }
    }

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
        val env = kisEnv(props)
        val accounts = parseAccounts(props.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.price.minute-candle-enabled=true에는 KIS_ACCOUNTS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(env.restBaseUrl, tokens, candleRestLimiters, gate)
        return MinuteCandleFetcher { code, to -> rest.minuteCandles(account, code, to) }
    }

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
    fun minuteCandleRefreshService(
        fetcher: MinuteCandleFetcher,
        store: MinuteCandleStore,
        calendar: MarketCalendar,
        refreshLock: MinuteRefreshLock,
        props: PriceProperties,
        meters: MeterRegistry,
    ): MinuteCandleRefreshService = MinuteCandleRefreshService(
        fetcher = fetcher,
        store = store,
        calendar = calendar,
        refreshLock = refreshLock,
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

    internal fun kisEnv(props: PriceProperties): KisEnv = KisEnv.valueOf(props.env.trim().uppercase())

    private companion object {
        val CANDLE_ACQUIRE_TIMEOUT: Duration = Duration.ofSeconds(10)
    }

    internal fun parseAccounts(accountsJson: String): List<KisAccount> = try {
        jacksonObjectMapper().readValue(accountsJson)
    } catch (e: Exception) {
        throw IllegalStateException(
            "KIS_ACCOUNTS JSON 파싱 실패 — [{\"keyId\",\"appkey\",\"appsecret\"}] 배열이어야 한다",
        )
    }
}
