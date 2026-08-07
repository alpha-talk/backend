package com.alphatalk.worker.batch.config

import com.alphatalk.kis.auth.KisTokenManager
import com.alphatalk.kis.auth.KisTokenStore
import com.alphatalk.kis.master.KisMasterClient
import com.alphatalk.kis.model.KisAccount
import com.alphatalk.kis.model.KisApi
import com.alphatalk.kis.model.KisLimits
import com.alphatalk.kis.rate.KisRateGate
import com.alphatalk.kis.rate.KisRateLimiters
import com.alphatalk.kis.rest.KisRestClient
import com.alphatalk.worker.batch.industry.HttpDartClient
import com.alphatalk.worker.batch.industry.IndustryStore
import com.alphatalk.worker.batch.industry.IndustrySyncJob
import com.alphatalk.worker.batch.industry.KsicCatalog
import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.kis.RedisKisRateGate
import com.alphatalk.worker.batch.kis.RedisKisTokenStore
import com.alphatalk.worker.batch.master.MasterFileFetcher
import com.alphatalk.worker.batch.master.StockMasterStore
import com.alphatalk.worker.batch.master.StockMasterSyncJob
import com.alphatalk.worker.batch.opinion.InvestOpinionStore
import com.alphatalk.worker.batch.opinion.InvestOpinionSyncJob
import com.alphatalk.worker.batch.opinion.KisMemberBrokerDirectory
import com.alphatalk.worker.batch.opinion.OpinionEventBinder
import com.alphatalk.worker.batch.opinion.OpinionFetcher
import com.alphatalk.worker.batch.opinion.OpinionPublisher
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.micrometer.core.instrument.MeterRegistry
import net.javacrumbs.shedlock.core.LockProvider
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
    @ConditionalOnProperty("alphatalk.batch.opinion.enabled", havingValue = "true")
    fun batchKisTokenStore(redis: StringRedisTemplate): KisTokenStore = RedisKisTokenStore(redis)

    @Bean
    @ConditionalOnProperty("alphatalk.batch.opinion.enabled", havingValue = "true")
    fun batchKisTokenManager(store: KisTokenStore): KisTokenManager =
        KisTokenManager(KisApi.REST_BASE_URL, store)

    @Bean
    @ConditionalOnProperty("alphatalk.batch.opinion.enabled", havingValue = "true")
    fun batchKisRateGate(props: BatchProperties, redis: StringRedisTemplate): KisRateGate {
        val rate = KisLimits.REST_CALLS_PER_SECOND * props.kis.rateFactor
        return RedisKisRateGate(
            redis = redis,
            capacity = ceil(rate).toInt().coerceAtLeast(1),
            refillPerSecond = rate,
        )
    }

    @Bean
    @ConditionalOnProperty("alphatalk.batch.opinion.enabled", havingValue = "true")
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
            requestInterval = props.opinion.requestInterval,
            scanLimit = props.opinion.scanLimit,
        )
    }

    internal fun parseAccounts(accountsJson: String): List<KisAccount> = try {
        jacksonObjectMapper().readValue(accountsJson)
    } catch (e: Exception) {
        throw IllegalStateException("alphatalk.batch.kis.accounts-json 파싱 실패", e)
    }
}
