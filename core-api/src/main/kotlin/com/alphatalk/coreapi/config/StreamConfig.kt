package com.alphatalk.coreapi.config

import com.alphatalk.coreapi.stream.QuoteStore
import com.alphatalk.coreapi.stream.StreamService
import com.alphatalk.coreapi.stream.StreamStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class StreamConfig {
    @Bean
    fun streamService(stream: StreamStore, quotes: QuoteStore): StreamService = StreamService(stream, quotes)
}
