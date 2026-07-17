package com.alphatalk.ws.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.listener.RedisMessageListenerContainer
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor

@Configuration
class RedisConfig {
    @Bean
    fun redisMessageListenerContainer(
        connectionFactory: RedisConnectionFactory,
        redisListenerExecutor: ThreadPoolTaskExecutor,
    ): RedisMessageListenerContainer = RedisMessageListenerContainer().apply {
        setConnectionFactory(connectionFactory)
        setTaskExecutor(redisListenerExecutor)
    }

    @Bean
    fun redisListenerExecutor(): ThreadPoolTaskExecutor = ThreadPoolTaskExecutor().apply {
        corePoolSize = 4
        maxPoolSize = 4
        setThreadNamePrefix("redis-listener-")
        initialize()
    }
}
