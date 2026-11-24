package com.alphatalk.worker.price.config

import com.alphatalk.contracts.Channels
import com.alphatalk.worker.price.candle.RedisMinuteRefreshLock
import com.alphatalk.worker.price.demand.RedisDemandSource
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.ChannelTopic
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.ThreadPoolExecutor

@Configuration(proxyBeanMethods = false)
@ConditionalOnKisAccounts
class PriceRedisListenerConfig {
    @Bean(defaultCandidate = false)
    fun priceRedisListenerExecutor(): ThreadPoolTaskExecutor = ThreadPoolTaskExecutor().apply {
        corePoolSize = 2
        maxPoolSize = 2
        setQueueCapacity(256)
        setRejectedExecutionHandler(ThreadPoolExecutor.CallerRunsPolicy())
        setThreadNamePrefix("price-redis-listener-")
    }

    @Bean
    fun priceRedisListenerContainer(
        connectionFactory: RedisConnectionFactory,
        demand: RedisDemandSource,
        refreshLock: RedisMinuteRefreshLock,
        @Qualifier("priceRedisListenerExecutor") executor: ThreadPoolTaskExecutor,
    ): RedisMessageListenerContainer = RedisMessageListenerContainer().apply {
        setConnectionFactory(connectionFactory)
        setTaskExecutor(executor)
        addMessageListener({ _, _ -> demand.signalRefresh() }, ChannelTopic(Channels.DEMAND_UPDATED))
        addMessageListener(refreshLock, ChannelTopic(Channels.MINUTE_REFRESH_COMPLETED))
    }
}
