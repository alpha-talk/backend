package com.alphatalk.coreapi.config

import com.alphatalk.auth.TokenVerifier
import com.alphatalk.coreapi.auth.JwtAuthenticationFilter
import com.alphatalk.coreapi.support.ApiErrorBody
import com.alphatalk.coreapi.support.ApiErrorResponse
import com.alphatalk.coreapi.support.ErrorCode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer
import org.springframework.security.config.http.SessionCreationPolicy
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.security.web.SecurityFilterChain
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter

@Configuration
class SecurityConfig {
    @Bean
    fun passwordEncoder(): PasswordEncoder = BCryptPasswordEncoder(BCRYPT_COST)

    @Bean
    fun securityFilterChain(
        http: HttpSecurity,
        verifier: TokenVerifier,
        mapper: ObjectMapper,
    ): SecurityFilterChain = http
        .csrf { it.disable() }
        .httpBasic { it.disable() }
        .formLogin { it.disable() }
        .sessionManagement { it.sessionCreationPolicy(SessionCreationPolicy.STATELESS) }
        .authorizeHttpRequests {
            it.requestMatchers(*PUBLIC_PATHS).permitAll()
                .anyRequest().authenticated()
        }
        .exceptionHandling { handling ->
            handling.authenticationEntryPoint { _, response, _ ->
                response.status = ErrorCode.UNAUTHORIZED.status.value()
                response.contentType = MediaType.APPLICATION_JSON_VALUE
                response.characterEncoding = Charsets.UTF_8.name()
                mapper.writeValue(
                    response.writer,
                    ApiErrorResponse(ApiErrorBody(ErrorCode.UNAUTHORIZED.name, "인증이 필요합니다")),
                )
            }
            handling.accessDeniedHandler { _, response, _ ->
                response.status = ErrorCode.FORBIDDEN.status.value()
                response.contentType = MediaType.APPLICATION_JSON_VALUE
                response.characterEncoding = Charsets.UTF_8.name()
                mapper.writeValue(
                    response.writer,
                    ApiErrorResponse(ApiErrorBody(ErrorCode.FORBIDDEN.name, "권한이 없습니다")),
                )
            }
        }
        .addFilterBefore(
            JwtAuthenticationFilter(verifier, mapper),
            UsernamePasswordAuthenticationFilter::class.java,
        )
        .build()

    companion object {
        private const val BCRYPT_COST = 10
        private val PUBLIC_PATHS = arrayOf(
            "/api/v1/auth/signup",
            "/api/v1/auth/login",
            "/api/v1/auth/refresh",
            "/actuator/health",
        )
    }
}
