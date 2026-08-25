package com.alphatalk.worker.batch.config

import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.auth.KisTokenStore
import com.alphatalk.kis.master.KisMasterClient
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisApi
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.alphatalk.kis.redis.RedisKisRateGate
import com.alphatalk.kis.redis.RedisKisTokenStore
import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.worker.batch.industry.HttpDartClient
import com.alphatalk.worker.batch.industry.IndustryStore
import com.alphatalk.worker.batch.industry.IndustrySyncJob
import com.alphatalk.worker.batch.industry.KsicCatalog
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.job.CatchUpTask
import com.alphatalk.worker.batch.job.StartupCatchUp
import com.alphatalk.worker.batch.master.MasterFileFetcher
import com.alphatalk.worker.batch.master.StockMasterStore
import com.alphatalk.worker.batch.master.StockMasterSyncJob
import com.alphatalk.worker.batch.financials.CorpDirectory
import com.alphatalk.worker.batch.financials.FinancialSummaryStore
import com.alphatalk.worker.batch.financials.FinancialsSyncJob
import com.alphatalk.worker.batch.opinion.InvestOpinionStore
import com.alphatalk.worker.batch.opinion.InvestOpinionSyncJob
import com.alphatalk.worker.batch.opinion.KisMemberBrokerDirectory
import com.alphatalk.worker.batch.opinion.OpinionEventBinder
import com.alphatalk.worker.batch.opinion.OpinionFetcher
import com.alphatalk.worker.batch.opinion.OpinionPublisher
import com.alphatalk.worker.batch.stockinfo.InvestorFlowFetcher
import com.alphatalk.worker.batch.stockinfo.InvestorFlowStore
import com.alphatalk.worker.batch.stockinfo.InvestorFlowSyncJob
import com.alphatalk.worker.batch.stockinfo.StockUniverse
import com.alphatalk.worker.batch.stockinfo.ValuationFetcher
import com.alphatalk.worker.batch.stockinfo.ValuationStore
import com.alphatalk.worker.batch.stockinfo.ValuationSyncJob
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.core.LockProvider
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.LocalDate
import kotlin.math.ceil

@Configuration
class BatchConfig {
    @Bean
    fun masterFileFetcher(props: BatchProperties): MasterFileFetcher {
        val client = KisMasterClient(props.stockMaster.baseUrl)
        return object : MasterFileFetcher {
            override fun fetch(market: com.alphatalk.kis.master.KisMarket) = client.download(market)
        }
    }

    @Bean
    @ConditionalOnExpression("\${alphatalk.batch.dart.enabled:true} and !'\${alphatalk.batch.dart.api-key:}'.isEmpty()")
    fun industrySyncJob(
        props: BatchProperties,
        ksic: KsicCatalog,
        store: IndustryStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
        mapper: ObjectMapper,
    ): IndustrySyncJob {
        val client = HttpDartClient(props.dart.apiKey, props.dart.baseUrl, mapper)
        return IndustrySyncJob(
            dart = client,
            ksic = ksic,
            store = store,
            runs = runs,
            meters = meters,
            requestInterval = props.dart.requestInterval,
            groupMaxSize = props.dart.groupMaxSize,
            groupOverrides = props.dart.groupOverrides,
            maxFailureRatio = props.dart.maxFailureRatio,
        )
    }

    @Bean
    @ConditionalOnProperty("alphatalk.batch.stock-master.enabled", havingValue = "true", matchIfMissing = true)
    fun stockMasterSyncJob(
        files: MasterFileFetcher,
        stocks: StockMasterStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
    ): StockMasterSyncJob = StockMasterSyncJob(files, stocks, runs, meters)

    @Bean
    @ConditionalOnExpression(KIS_JOB_ENABLED)
    fun batchKisTokenStore(redis: StringRedisTemplate): KisTokenStore = RedisKisTokenStore(redis)

    @Bean
    @ConditionalOnExpression(KIS_JOB_ENABLED)
    fun batchKisTokenManager(store: KisTokenStore): KisTokenManager =
        KisTokenManager(KisApi.REST_BASE_URL, store)

    @Bean
    @ConditionalOnExpression(KIS_JOB_ENABLED)
    fun batchKisRateGate(props: BatchProperties, redis: StringRedisTemplate): KisRateGate {
        val rate = KisLimits.REST_CALLS_PER_SECOND * props.kis.rateFactor
        return RedisKisRateGate(
            redis = redis,
            capacity = ceil(rate).toInt().coerceAtLeast(1),
            refillPerSecond = rate,
        )
    }

    @Bean
    @ConditionalOnExpression(OPINION_JOB_ENABLED)
    fun investOpinionSyncJob(
        props: BatchProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
        store: InvestOpinionStore,
        binder: OpinionEventBinder,
        publisher: OpinionPublisher,
        runs: BatchJobRunStore,
        locks: LockProvider,
        meters: MeterRegistry,
    ): InvestOpinionSyncJob {
        val accounts = parseAccounts(props.kis.accountsJson)
        check(accounts.isNotEmpty()) {
            "alphatalk.batch.opinion.enabled=true에는 KIS 계정이 최소 1개 필요하다"
        }
        val account = accounts.first()
        val rest = KisRestClient(
            KisApi.REST_BASE_URL,
            tokens,
            KisRateLimiters(props.opinion.callsPerSecond, 1.0),
            gate,
        )
        val members = KisMasterClient(props.stockMaster.baseUrl)
        return InvestOpinionSyncJob(
            brokers = KisMemberBrokerDirectory({ members.downloadMembers() }),
            fetcher = OpinionFetcher { broker, from, to -> rest.investOpinions(account, broker.queryCode, from, to) },
            store = store,
            binder = binder,
            publisher = publisher,
            runs = runs,
            locks = locks,
            meters = meters,
            holidays = props.holidays.map(LocalDate::parse).toSet(),
            businessDayEnforced = props.opinion.businessDayEnforced,
            requestInterval = props.opinion.requestInterval,
            scanLimit = props.opinion.scanLimit,
        )
    }

    @Bean
    @ConditionalOnExpression(VALUATION_JOB_ENABLED)
    fun valuationSyncJob(
        props: BatchProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
        universe: StockUniverse,
        store: ValuationStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
        master: ObjectProvider<StockMasterSyncJob>,
    ): ValuationSyncJob {
        val account = requireAccount(props, "alphatalk.batch.valuation.enabled")
        val rest = KisRestClient(
            KisApi.REST_BASE_URL,
            tokens,
            KisRateLimiters(props.valuation.callsPerSecond, 1.0),
            gate,
        )
        return ValuationSyncJob(
            universe = universe,
            fetcher = ValuationFetcher { code -> rest.valuationSnapshot(account, code) },
            store = store,
            runs = runs,
            meters = meters,
            holidays = props.holidays.map(LocalDate::parse).toSet(),
            prerequisiteJob = masterJobNameIfEnabled(master),
            chunkSize = props.valuation.chunkSize,
        )
    }

    @Bean
    @ConditionalOnExpression(INVESTOR_JOB_ENABLED)
    fun investorFlowSyncJob(
        props: BatchProperties,
        tokens: KisTokenManager,
        gate: KisRateGate,
        universe: StockUniverse,
        store: InvestorFlowStore,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
        master: ObjectProvider<StockMasterSyncJob>,
    ): InvestorFlowSyncJob {
        val account = requireAccount(props, "alphatalk.batch.investor.enabled")
        val rest = KisRestClient(
            KisApi.REST_BASE_URL,
            tokens,
            KisRateLimiters(props.investor.callsPerSecond, 1.0),
            gate,
        )
        return InvestorFlowSyncJob(
            universe = universe,
            fetcher = InvestorFlowFetcher { code -> rest.investorFlows(account, code) },
            store = store,
            runs = runs,
            meters = meters,
            holidays = props.holidays.map(LocalDate::parse).toSet(),
            prerequisiteJob = masterJobNameIfEnabled(master),
        )
    }

    @Bean
    @ConditionalOnProperty("alphatalk.batch.financials.enabled", havingValue = "true")
    fun financialsSyncJob(
        props: BatchProperties,
        universe: StockUniverse,
        store: FinancialSummaryStore,
        corps: CorpDirectory,
        runs: BatchJobRunStore,
        meters: MeterRegistry,
        mapper: ObjectMapper,
        master: ObjectProvider<StockMasterSyncJob>,
    ): FinancialsSyncJob {
        check(props.dart.apiKey.isNotBlank()) {
            "alphatalk.batch.financials.enabled=true에는 DART API 키가 필요하다"
        }
        val client = HttpDartClient(props.dart.apiKey, props.dart.baseUrl, mapper)
        return FinancialsSyncJob(
            dart = client,
            universe = universe,
            store = store,
            corps = corps,
            runs = runs,
            meters = meters,
            lookbackDays = props.financials.lookbackDays,
            backfillYears = props.financials.backfillYears,
            backfillPerRun = props.financials.backfillPerRun,
            failureStreakLimit = props.financials.failureStreakLimit,
            prerequisiteJob = masterJobNameIfEnabled(master),
            requestInterval = props.dart.requestInterval,
        )
    }

    @Bean
    @ConditionalOnExpression(
        "\${alphatalk.batch.enabled:true} and \${alphatalk.batch.catch-up.enabled:true}",
    )
    fun startupCatchUp(
        master: ObjectProvider<StockMasterSyncJob>,
        valuation: ObjectProvider<ValuationSyncJob>,
        investor: ObjectProvider<InvestorFlowSyncJob>,
        financials: ObjectProvider<FinancialsSyncJob>,
        props: BatchProperties,
        meters: MeterRegistry,
    ): StartupCatchUp {
        val tasks = buildList {
            master.ifAvailable?.let { job ->
                add(CatchUpTask(StockMasterSyncJob.JOB_NAME, props.stockMaster.cron) { job.scheduled() })
            }
            valuation.ifAvailable?.let { job ->
                add(CatchUpTask(ValuationSyncJob.JOB_NAME, props.valuation.cron) { job.scheduled() })
            }
            investor.ifAvailable?.let { job ->
                add(CatchUpTask(InvestorFlowSyncJob.JOB_NAME, props.investor.cron) { job.scheduled() })
            }
            financials.ifAvailable?.let { job ->
                add(CatchUpTask(FinancialsSyncJob.JOB_NAME, props.financials.cron) { job.scheduled() })
            }
        }
        return StartupCatchUp(
            tasks = tasks,
            meters = meters,
            passes = props.catchUp.passes,
            passInterval = props.catchUp.passInterval,
            stopTimeout = props.catchUp.stopTimeout,
        )
    }

    private fun masterJobNameIfEnabled(master: ObjectProvider<StockMasterSyncJob>): String? =
        master.ifAvailable?.let { StockMasterSyncJob.JOB_NAME }

    private fun requireAccount(props: BatchProperties, flag: String): KisAccount {
        val accounts = parseAccounts(props.kis.accountsJson)
        check(accounts.isNotEmpty()) { "$flag=true에는 KIS 계정이 최소 1개 필요하다" }
        return accounts.first()
    }

    internal fun parseAccounts(accountsJson: String): List<KisAccount> = try {
        jacksonObjectMapper().readValue(accountsJson)
    } catch (e: Exception) {
        throw IllegalStateException("alphatalk.batch.kis.accounts-json 파싱 실패", e)
    }

    companion object {
        private const val OPINION_JOB_ENABLED =
            "\${alphatalk.batch.enabled:true} and \${alphatalk.batch.opinion.enabled:false}"

        private const val VALUATION_JOB_ENABLED =
            "\${alphatalk.batch.enabled:true} and \${alphatalk.batch.valuation.enabled:false}"

        private const val INVESTOR_JOB_ENABLED =
            "\${alphatalk.batch.enabled:true} and \${alphatalk.batch.investor.enabled:false}"

        private const val KIS_JOB_ENABLED =
            "\${alphatalk.batch.enabled:true} and (\${alphatalk.batch.opinion.enabled:false}" +
                " or \${alphatalk.batch.valuation.enabled:false}" +
                " or \${alphatalk.batch.investor.enabled:false})"
    }
}
