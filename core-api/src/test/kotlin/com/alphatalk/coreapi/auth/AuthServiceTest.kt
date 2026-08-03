package com.alphatalk.coreapi.auth

import com.alphatalk.auth.TokenIssuer
import com.alphatalk.coreapi.support.ApiException
import com.alphatalk.coreapi.support.ErrorCode
import org.springframework.security.crypto.password.PasswordEncoder
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue

class AuthServiceTest {
    private val now = Instant.parse("2026-07-29T00:00:00Z")

    private class InMemoryUserStore : UserStore {
        private val rows = mutableMapOf<Long, UserRecord>()
        private var nextId = 1L

        override fun create(email: String, passwordHash: String, nickname: String): Long {
            if (rows.values.any { it.email == email || it.nickname == nickname }) {
                throw DuplicateUserException(IllegalStateException("dup"))
            }
            val id = nextId++
            rows[id] = UserRecord(id, email, passwordHash, nickname, Instant.parse("2026-07-01T00:00:00Z"))
            return id
        }

        override fun findByEmail(email: String) = rows.values.firstOrNull { it.email == email }
        override fun findById(id: Long) = rows[id]
        override fun existsByEmail(email: String) = rows.values.any { it.email == email }
        override fun existsByNickname(nickname: String) = rows.values.any { it.nickname == nickname }
        override fun nicknames(ids: Collection<Long>) =
            rows.filterKeys(ids::contains).mapValues { (_, user) -> user.nickname }
    }

    private class InMemoryRefreshStore : RefreshTokenStore {
        val rows = mutableMapOf<String, RefreshTokenRecord>()
        val rotations = mutableListOf<Pair<String, String?>>()

        override fun save(userId: Long, tokenHash: String, expiresAt: Instant, rotatedFrom: String?) {
            rows[tokenHash] = RefreshTokenRecord(userId, tokenHash, expiresAt, null)
            rotations += tokenHash to rotatedFrom
        }

        override fun find(tokenHash: String) = rows[tokenHash]

        override fun revoke(tokenHash: String, at: Instant): Boolean {
            val row = rows[tokenHash] ?: return false
            if (row.revokedAt != null) return false
            rows[tokenHash] = row.copy(revokedAt = at)
            return true
        }

        override fun revokeAllOf(userId: Long, at: Instant): Int {
            var count = 0
            rows.forEach { (hash, row) ->
                if (row.userId == userId && row.revokedAt == null) {
                    rows[hash] = row.copy(revokedAt = at)
                    count++
                }
            }
            return count
        }
    }

    private class PlainPasswordEncoder : PasswordEncoder {
        override fun encode(rawPassword: CharSequence) = "hashed:$rawPassword"
        override fun matches(rawPassword: CharSequence, encodedPassword: String) =
            encodedPassword == "hashed:$rawPassword"
    }

    private val users = InMemoryUserStore()
    private val refreshTokens = InMemoryRefreshStore()
    private val issuer = object : TokenIssuer {
        override fun issue(userId: Long, ttl: Duration): String = "access-$userId-${ttl.seconds}"
    }
    private val service = AuthService(
        users = users,
        refreshTokens = refreshTokens,
        issuer = issuer,
        passwords = PlainPasswordEncoder(),
        props = AuthProperties(),
        clock = Clock.fixed(now, ZoneOffset.UTC),
    )

    private fun signupAndLogin(): TokenPair {
        service.signup(SignupRequest("a@b.c", "password1", "민균"))
        return service.login(LoginRequest("a@b.c", "password1"))
    }

    @Test
    fun `가입하면 비밀번호를 평문으로 두지 않는다`() {
        val id = service.signup(SignupRequest("a@b.c", "password1", "민균"))

        val stored = users.findById(id)!!
        assertNotEquals("password1", stored.passwordHash)
        assertTrue(stored.passwordHash.startsWith("hashed:"))
    }

    @Test
    fun `이메일이나 닉네임이 겹치면 가입할 수 없다`() {
        service.signup(SignupRequest("a@b.c", "password1", "민균"))

        val sameEmail = assertFailsWith<ApiException> {
            service.signup(SignupRequest("a@b.c", "password1", "다른닉"))
        }
        val sameNickname = assertFailsWith<ApiException> {
            service.signup(SignupRequest("other@b.c", "password1", "민균"))
        }

        assertEquals(ErrorCode.DUPLICATE, sameEmail.code)
        assertEquals(ErrorCode.DUPLICATE, sameNickname.code)
    }

    @Test
    fun `로그인하면 액세스와 리프레시를 함께 준다`() {
        val tokens = signupAndLogin()

        assertEquals("access-1-1800", tokens.accessToken)
        assertEquals(1800, tokens.accessExpiresIn)
        assertTrue(tokens.refreshToken.isNotBlank())
    }

    @Test
    fun `리프레시 토큰은 액세스 토큰과 다른 값이고 원문이 저장되지 않는다`() {
        val tokens = signupAndLogin()

        assertNotEquals(tokens.accessToken, tokens.refreshToken)
        assertTrue(refreshTokens.rows.keys.none { it == tokens.refreshToken })
        assertEquals(64, refreshTokens.rows.keys.first().length)
    }

    @Test
    fun `비밀번호가 틀리면 로그인에 실패한다`() {
        service.signup(SignupRequest("a@b.c", "password1", "민균"))

        val e = assertFailsWith<ApiException> { service.login(LoginRequest("a@b.c", "wrongpass1")) }

        assertEquals(ErrorCode.UNAUTHORIZED, e.code)
    }

    @Test
    fun `없는 계정도 같은 응답으로 막는다`() {
        val e = assertFailsWith<ApiException> { service.login(LoginRequest("nobody@b.c", "password1")) }

        assertEquals(ErrorCode.UNAUTHORIZED, e.code)
    }

    @Test
    fun `재발급하면 새 쌍을 주고 쓰던 리프레시는 무효가 된다`() {
        val first = signupAndLogin()

        val second = service.refresh(RefreshRequest(first.refreshToken))

        assertNotEquals(first.refreshToken, second.refreshToken)
        val rotated = refreshTokens.rotations.last()
        assertNotEquals(null, rotated.second)
        assertFailsWith<ApiException> { service.refresh(RefreshRequest(first.refreshToken)) }
    }

    @Test
    fun `이미 쓴 리프레시를 다시 내밀면 그 계정의 세션을 전부 끊는다`() {
        val first = signupAndLogin()
        val second = service.refresh(RefreshRequest(first.refreshToken))

        assertFailsWith<ApiException> { service.refresh(RefreshRequest(first.refreshToken)) }

        assertFailsWith<ApiException> { service.refresh(RefreshRequest(second.refreshToken)) }
        assertTrue(refreshTokens.rows.values.all { it.revokedAt != null })
    }

    @Test
    fun `만료된 리프레시는 거부한다`() {
        val expiringService = AuthService(
            users = users,
            refreshTokens = refreshTokens,
            issuer = issuer,
            passwords = PlainPasswordEncoder(),
            props = AuthProperties(refreshTtl = Duration.ofSeconds(1)),
            clock = Clock.fixed(now, ZoneOffset.UTC),
        )
        expiringService.signup(SignupRequest("a@b.c", "password1", "민균"))
        val tokens = expiringService.login(LoginRequest("a@b.c", "password1"))

        val later = AuthService(
            users = users,
            refreshTokens = refreshTokens,
            issuer = issuer,
            passwords = PlainPasswordEncoder(),
            props = AuthProperties(),
            clock = Clock.fixed(now.plusSeconds(60), ZoneOffset.UTC),
        )

        assertFailsWith<ApiException> { later.refresh(RefreshRequest(tokens.refreshToken)) }
    }

    @Test
    fun `모르는 리프레시 토큰은 거부한다`() {
        assertFailsWith<ApiException> { service.refresh(RefreshRequest("made-up-token")) }
    }

    @Test
    fun `로그아웃하면 그 계정의 리프레시가 모두 끊긴다`() {
        val tokens = signupAndLogin()

        service.logout(userId = 1L)

        assertFailsWith<ApiException> { service.refresh(RefreshRequest(tokens.refreshToken)) }
    }

    @Test
    fun `내 프로필은 가입 정보를 그대로 돌려준다`() {
        val id = service.signup(SignupRequest("a@b.c", "password1", "민균"))

        val me = service.me(id)

        assertEquals(id, me.userId)
        assertEquals("a@b.c", me.email)
        assertEquals("민균", me.nickname)
        assertEquals(Instant.parse("2026-07-01T00:00:00Z").toEpochMilli(), me.createdAt)
    }

}
