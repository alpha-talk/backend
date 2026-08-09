package com.alphatalk.worker.batch.config

import com.alphatalk.worker.batch.job.BatchJobRunStore
import com.alphatalk.worker.batch.opinion.InvestOpinionStore
import com.alphatalk.worker.batch.opinion.OpinionEventBinder
import com.alphatalk.worker.batch.opinion.OpinionObservation
import com.alphatalk.worker.batch.opinion.OpinionPublisher
import com.alphatalk.worker.batch.opinion.UnpublishedOpinion
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.core.SimpleLock
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.context.PropertyPlaceholderAutoConfiguration
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Instant
import java.util.Optional
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class BatchConfigConditionTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(PropertyPlaceholderAutoConfiguration::class.java))
        .withUserConfiguration(StubDependencies::class.java, BatchConfig::class.java)
        .withPropertyValues(
            "alphatalk.batch.stock-master.enabled=false",
            "alphatalk.batch.dart.enabled=false",
            "alphatalk.batch.kis.accounts-json=[{\"keyId\":\"k\",\"appkey\":\"a\",\"appsecret\":\"s\"}]",
        )

    @Test
    fun `두 플래그가 모두 켜지면 투자의견 잡이 만들어진다`() {
        runner.withPropertyValues(
            "alphatalk.batch.enabled=true",
            "alphatalk.batch.opinion.enabled=true",
        ).run { context ->
            assertTrue(context.containsBean("investOpinionSyncJob"))
        }
    }

    @Test
    fun `전역 배치가 꺼져 있으면 만들지 않는다 - LockProvider 부재로 기동이 깨지지 않게`() {
        runner.withPropertyValues(
            "alphatalk.batch.enabled=false",
            "alphatalk.batch.opinion.enabled=true",
        ).run { context ->
            assertFalse(context.containsBean("investOpinionSyncJob"))
        }
    }

    @Test
    fun `투자의견이 꺼져 있으면 전역이 켜져 있어도 만들지 않는다`() {
        runner.withPropertyValues("alphatalk.batch.enabled=true").run { context ->
            assertFalse(context.containsBean("investOpinionSyncJob"))
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(BatchProperties::class)
    class StubDependencies {
        @Bean
        fun meterRegistry(): MeterRegistry = SimpleMeterRegistry()

        @Bean
        fun lockProvider(): LockProvider = LockProvider {
            Optional.of(SimpleLock { })
        }

        @Bean
        fun batchJobRunStore(): BatchJobRunStore = object : BatchJobRunStore {
            override fun start(job: String, runDate: String, startedAt: Instant): Long = 1L
            override fun startOrRepair(job: String, runDate: String, startedAt: Instant): Long = 1L
            override fun restart(job: String, runDate: String, startedAt: Instant): Long = 1L
            override fun succeed(id: Long, okCount: Int, failCount: Int, finishedAt: Instant) {}
            override fun fail(id: Long, error: String, finishedAt: Instant) {}
            override fun failCounted(id: Long, okCount: Int, failCount: Int, error: String, finishedAt: Instant) {}
            override fun hasCleanSuccess(job: String, runDate: String): Boolean = false
        }

        @Bean
        fun investOpinionStore(): InvestOpinionStore = object : InvestOpinionStore {
            override fun insertIfAbsent(observation: OpinionObservation): Boolean = false
            override fun findUnpublished(limit: Int): List<UnpublishedOpinion> = emptyList()
            override fun markPublished(streamEventId: String, at: Instant): Boolean = false
        }

        @Bean
        fun opinionEventBinder(): OpinionEventBinder = object : OpinionEventBinder {
            override fun ensureEvent(opinion: UnpublishedOpinion): String = "01TEST0000000000000000000"
        }

        @Bean
        fun opinionPublisher(): OpinionPublisher = OpinionPublisher { _, _, _ -> true }

        @Bean
        fun stringRedisTemplate(): StringRedisTemplate =
            StringRedisTemplate(LettuceConnectionFactory())
    }
}
