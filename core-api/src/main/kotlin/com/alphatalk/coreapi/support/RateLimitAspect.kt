package com.alphatalk.coreapi.support

import org.aspectj.lang.JoinPoint
import org.aspectj.lang.annotation.Aspect
import org.aspectj.lang.annotation.Before
import org.aspectj.lang.reflect.MethodSignature
import org.springframework.context.expression.MethodBasedEvaluationContext
import org.springframework.core.DefaultParameterNameDiscoverer
import org.springframework.expression.Expression
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.stereotype.Component
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Aspect
@Component
class RateLimitAspect(
    private val rateLimiter: RateLimiter,
) {
    private val parser = SpelExpressionParser()
    private val parameterNames = DefaultParameterNameDiscoverer()
    private val expressions = ConcurrentHashMap<String, Expression>()

    @Before("@annotation(rateLimited)")
    fun enforce(joinPoint: JoinPoint, rateLimited: RateLimited) {
        val decision = rateLimiter.tryAcquire(
            action = rateLimited.action,
            key = resolveKey(joinPoint, rateLimited),
            limit = rateLimited.limit,
            window = Duration.ofSeconds(rateLimited.windowSeconds),
        )
        if (!decision.allowed) {
            throw RateLimitExceededException(rateLimited.action, decision.retryAfterSeconds)
        }
    }

    private fun resolveKey(joinPoint: JoinPoint, rateLimited: RateLimited): String {
        if (rateLimited.key.isEmpty()) return CurrentUser.id().toString()
        val method = (joinPoint.signature as MethodSignature).method
        val context = MethodBasedEvaluationContext(joinPoint.target, method, joinPoint.args, parameterNames)
        val expression = expressions.computeIfAbsent(rateLimited.key, parser::parseExpression)
        return expression.getValue(context, String::class.java)
            ?: throw IllegalStateException("rate limit key expression resolved to null: ${rateLimited.key}")
    }
}
