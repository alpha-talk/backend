package com.alphatalk.auth.spring

import com.alphatalk.auth.JwtTokenProvider
import com.alphatalk.auth.TokenVerifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

@AutoConfiguration
@EnableConfigurationProperties(JwtAuthProperties::class)
class JwtAuthAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean(TokenVerifier::class)
    fun jwtTokenProvider(props: JwtAuthProperties): JwtTokenProvider =
        JwtTokenProvider(props.secret)
}

@ConfigurationProperties(prefix = "alphatalk.auth.jwt")
data class JwtAuthProperties(
    val secret: String,
)
