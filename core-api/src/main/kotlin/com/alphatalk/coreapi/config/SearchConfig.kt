package com.alphatalk.coreapi.config

import com.alphatalk.coreapi.search.StockSearchService
import com.alphatalk.coreapi.search.StockSearchStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SearchConfig {
    @Bean
    fun stockSearchService(store: StockSearchStore): StockSearchService = StockSearchService(store)
}
