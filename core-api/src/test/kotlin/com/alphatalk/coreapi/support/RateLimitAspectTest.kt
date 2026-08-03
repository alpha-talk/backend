package com.alphatalk.coreapi.support

import jakarta.servlet.http.HttpServletRequest
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory
import org.springframework.mock.web.MockHttpServletRequest
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken
import org.springframework.security.core.context.SecurityContextHolder
import java.time.Duration
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RateLimitAspectTest {
    private class RecordingRateLimiter(private val allowed: Boolean = true) : RateLimiter {
        val calls = mutableListOf<Call>()

        data class Call(val action: String, val key: String, val limit: Int, val window: Duration)

        override fun tryAcquire(action: String, key: String, limit: Int, window: Duration): RateLimitDecision {
            calls += Call(action, key, limit, window)
            return RateLimitDecision(allowed, retryAfterSeconds = 17)
        }
    }

    open class Endpoints {
        var invocations = 0

        @RateLimited(action = "post", limit = 5)
        open fun byCurrentUser() {
            invocations++
        }

        @RateLimited(action = "login", limit = 10, key = "#http.remoteAddr + ':' + #request.email")
        open fun byExpression(request: Credentials, http: HttpServletRequest) {
            invocations++
        }

        @RateLimited(action = "like", limit = 60, windowSeconds = 10)
        open fun withCustomWindow() {
            invocations++
        }

        open fun unannotated() {
            invocations++
        }
    }

    data class Credentials(val email: String)

    private fun proxy(limiter: RateLimiter, target: Endpoints = Endpoints()): Endpoints {
        val factory = AspectJProxyFactory(target)
        factory.addAspect(RateLimitAspect(limiter))
        return factory.getProxy()
    }

    private fun authenticate(userId: Long) {
        SecurityContextHolder.getContext().authentication =
            UsernamePasswordAuthenticationToken(userId, null, emptyList())
    }

    private fun servletRequest(ip: String) = MockHttpServletRequest().apply { remoteAddr = ip }

    @AfterTest
    fun clearContext() {
        SecurityContextHolder.clearContext()
    }

    @Test
    fun `키를 지정하지 않으면 현재 로그인 유저로 센다`() {
        val limiter = RecordingRateLimiter()
        authenticate(42)

        proxy(limiter).byCurrentUser()

        assertEquals(
            RecordingRateLimiter.Call("post", "42", 5, Duration.ofMinutes(1)),
            limiter.calls.single(),
        )
    }

    @Test
    fun `SpEL 키는 메서드 인자에서 조합한다`() {
        val limiter = RecordingRateLimiter()

        proxy(limiter).byExpression(Credentials("a@b.c"), servletRequest("10.0.0.1"))

        assertEquals("10.0.0.1:a@b.c", limiter.calls.single().key)
        assertEquals("login", limiter.calls.single().action)
    }

    @Test
    fun `한도를 넘기면 429와 재시도 시각을 던지고 본문을 실행하지 않는다`() {
        val target = Endpoints()
        authenticate(42)

        val failure = assertFailsWith<RateLimitExceededException> {
            proxy(RecordingRateLimiter(allowed = false), target).byCurrentUser()
        }

        assertEquals(ErrorCode.RATE_LIMITED, failure.code)
        assertEquals(17, failure.retryAfterSeconds)
        assertEquals(0, target.invocations, "한도 초과인데 엔드포인트가 실행됐다")
    }

    @Test
    fun `허용되면 본문이 그대로 실행된다`() {
        val target = Endpoints()
        authenticate(42)

        proxy(RecordingRateLimiter(), target).byCurrentUser()

        assertEquals(1, target.invocations)
    }

    @Test
    fun `윈도는 애노테이션이 정한 값을 쓴다`() {
        val limiter = RecordingRateLimiter()
        authenticate(42)

        proxy(limiter).withCustomWindow()

        assertEquals(Duration.ofSeconds(10), limiter.calls.single().window)
    }

    @Test
    fun `애노테이션이 없는 엔드포인트는 유량을 세지 않는다`() {
        val limiter = RecordingRateLimiter()

        proxy(limiter).unannotated()

        assertTrue(limiter.calls.isEmpty())
    }

    @Test
    fun `인증 없이 유저 키를 요구하면 401이다`() {
        val failure = assertFailsWith<ApiException> { proxy(RecordingRateLimiter()).byCurrentUser() }

        assertEquals(ErrorCode.UNAUTHORIZED, failure.code)
    }
}
