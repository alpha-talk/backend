package com.alphatalk.coreapi.config

import com.alphatalk.coreapi.subscription.RedisWatchlistBroadcaster
import com.alphatalk.coreapi.subscription.StockCatalog
import com.alphatalk.coreapi.subscription.WatchlistAnnouncer
import com.alphatalk.coreapi.subscription.WatchlistMirror
import com.alphatalk.coreapi.subscription.WatchlistService
import com.alphatalk.coreapi.subscription.WatchlistStore
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Clock

@Configuration
class SubscriptionConfig {
    @Bean
    fun watchlistBroadcaster(
        redis: StringRedisTemplate,
        mapper: ObjectMapper,
    ): RedisWatchlistBroadcaster = RedisWatchlistBroadcaster(redis, mapper, Clock.systemUTC())

    @Bean
    fun watchlistService(
        store: WatchlistStore,
        catalog: StockCatalog,
        mirror: WatchlistMirror,
        announcer: WatchlistAnnouncer,
    ): WatchlistService = WatchlistService(store, catalog, mirror, announcer)
}
