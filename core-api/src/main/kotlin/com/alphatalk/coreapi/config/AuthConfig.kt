package com.alphatalk.coreapi.config

import com.alphatalk.auth.TokenIssuer
import com.alphatalk.coreapi.auth.AuthProperties
import com.alphatalk.coreapi.auth.AuthService
import com.alphatalk.coreapi.auth.RefreshTokenStore
import com.alphatalk.coreapi.auth.UserStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.crypto.password.PasswordEncoder

@Configuration
class AuthConfig {
    @Bean
    fun authService(
        users: UserStore,
        refreshTokens: RefreshTokenStore,
        issuer: TokenIssuer,
        passwords: PasswordEncoder,
        props: AuthProperties,
    ): AuthService = AuthService(users, refreshTokens, issuer, passwords, props)
}
