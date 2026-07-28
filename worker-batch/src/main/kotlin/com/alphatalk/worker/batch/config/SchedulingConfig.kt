package com.alphatalk.worker.batch.config

import net.javacrumbs.shedlock.core.LockProvider
import net.javacrumbs.shedlock.provider.redis.spring.RedisLockProvider
import net.javacrumbs.shedlock.spring.annotation.EnableSchedulerLock
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.scheduling.annotation.EnableScheduling

@Configuration
@EnableScheduling
@EnableSchedulerLock(defaultLockAtMostFor = "PT30M")
@ConditionalOnProperty("alphatalk.batch.enabled", havingValue = "true", matchIfMissing = true)
class SchedulingConfig {
    @Bean
    fun lockProvider(connectionFactory: RedisConnectionFactory): LockProvider =
        RedisLockProvider(connectionFactory, LOCK_ENVIRONMENT)

    companion object {
        private const val LOCK_ENVIRONMENT = "alphatalk"
    }
}
