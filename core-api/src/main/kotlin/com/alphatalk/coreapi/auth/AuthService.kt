package com.alphatalk.coreapi.auth

import com.alphatalk.auth.TokenIssuer
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.slf4j.LoggerFactory
import org.springframework.security.crypto.password.PasswordEncoder
import java.security.MessageDigest
import java.security.SecureRandom
import java.time.Clock
import java.util.Base64

class AuthService(
    private val users: UserStore,
    private val refreshTokens: RefreshTokenStore,
    private val issuer: TokenIssuer,
    private val passwords: PasswordEncoder,
    private val props: AuthProperties,
    private val clock: Clock = Clock.systemUTC(),
    private val random: SecureRandom = SecureRandom(),
) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun signup(request: SignupRequest): Long {
        if (users.existsByEmail(request.email)) {
            throw ApiException(ErrorCode.DUPLICATE, "이미 가입된 이메일입니다", mapOf("field" to "email"))
        }
        if (users.existsByNickname(request.nickname)) {
            throw ApiException(ErrorCode.DUPLICATE, "이미 사용 중인 닉네임입니다", mapOf("field" to "nickname"))
        }
        return try {
            users.create(request.email, passwords.encode(request.password), request.nickname)
        } catch (e: DuplicateUserException) {
            throw ApiException(ErrorCode.DUPLICATE, "이미 가입된 이메일 또는 닉네임입니다")
        }
    }

    fun login(request: LoginRequest): TokenPair {
        val user = users.findByEmail(request.email)
        if (user == null || !passwords.matches(request.password, user.passwordHash)) {
            throw ApiException(ErrorCode.UNAUTHORIZED, "이메일 또는 비밀번호가 올바르지 않습니다")
        }
        return issuePair(user.id, rotatedFrom = null)
    }

    fun refresh(request: RefreshRequest): TokenPair {
        val presentedHash = hash(request.refreshToken)
        val stored = refreshTokens.find(presentedHash)
            ?: throw ApiException(ErrorCode.UNAUTHORIZED, "리프레시 토큰이 유효하지 않습니다")
        val now = clock.instant()
        if (stored.revokedAt != null) {
            refreshTokens.revokeAllOf(stored.userId, now)
            log.warn("refresh token reuse detected, revoking all sessions: userId={}", stored.userId)
            throw ApiException(ErrorCode.UNAUTHORIZED, "리프레시 토큰이 재사용되어 모든 세션을 종료했습니다")
        }
        if (!stored.expiresAt.isAfter(now)) {
            throw ApiException(ErrorCode.UNAUTHORIZED, "리프레시 토큰이 만료되었습니다")
        }
        refreshTokens.revoke(presentedHash, now)
        return issuePair(stored.userId, rotatedFrom = presentedHash)
    }

    fun logout(userId: Long) {
        refreshTokens.revokeAllOf(userId, clock.instant())
    }

    fun me(userId: Long): MeResponse {
        val user = users.findById(userId)
            ?: throw ApiException(ErrorCode.UNAUTHORIZED, "인증이 필요합니다")
        return MeResponse(
            userId = user.id,
            email = user.email,
            nickname = user.nickname,
            createdAt = user.createdAt.toEpochMilli(),
        )
    }

    private fun issuePair(userId: Long, rotatedFrom: String?): TokenPair {
        val accessToken = issuer.issue(userId, props.accessTtl)
        val refreshToken = newRefreshToken()
        refreshTokens.save(
            userId = userId,
            tokenHash = hash(refreshToken),
            expiresAt = clock.instant() + props.refreshTtl,
            rotatedFrom = rotatedFrom,
        )
        return TokenPair(accessToken, refreshToken, props.accessTtl.seconds)
    }

    private fun newRefreshToken(): String {
        val bytes = ByteArray(REFRESH_TOKEN_BYTES)
        random.nextBytes(bytes)
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes)
    }

    private fun hash(token: String): String =
        MessageDigest.getInstance("SHA-256")
            .digest(token.toByteArray(Charsets.UTF_8))
            .joinToString("") { "%02x".format(it) }

    companion object {
        private const val REFRESH_TOKEN_BYTES = 32
    }
}
