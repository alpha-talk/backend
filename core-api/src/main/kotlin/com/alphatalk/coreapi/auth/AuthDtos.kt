package com.alphatalk.coreapi.auth

import jakarta.validation.constraints.Email
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern
import jakarta.validation.constraints.Size

data class SignupRequest(
    @field:Email(message = "이메일 형식이 아닙니다")
    @field:NotBlank(message = "이메일은 필수입니다")
    val email: String,
    @field:Pattern(
        regexp = "^(?=.*[A-Za-z])(?=.*\\d).{8,72}$",
        message = "비밀번호는 8자 이상이고 영문과 숫자를 모두 포함해야 합니다",
    )
    val password: String,
    @field:Size(min = 2, max = 12, message = "닉네임은 2~12자여야 합니다")
    val nickname: String,
)

data class SignupResponse(val userId: Long)

data class LoginRequest(
    @field:NotBlank(message = "이메일은 필수입니다")
    val email: String,
    @field:NotBlank(message = "비밀번호는 필수입니다")
    val password: String,
)

data class RefreshRequest(
    @field:NotBlank(message = "리프레시 토큰은 필수입니다")
    val refreshToken: String,
)

data class TokenPair(
    val accessToken: String,
    val refreshToken: String,
    val accessExpiresIn: Long,
)

data class MeResponse(
    val userId: Long,
    val email: String,
    val nickname: String,
    val createdAt: Long,
)
