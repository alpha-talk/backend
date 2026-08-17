package com.alphatalk.worker.price.config

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.springframework.context.annotation.Condition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.core.type.AnnotatedTypeMetadata

@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@Conditional(KisAccountsCondition::class)
annotation class ConditionalOnKisAccounts

class KisAccountsCondition : Condition {

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        val raw = context.environment.getProperty(ACCOUNTS_PROPERTY)?.trim().orEmpty()
        if (raw.isEmpty()) return false
        val parsed = runCatching { jacksonObjectMapper().readTree(raw) }.getOrNull() ?: return true
        return !(parsed.isArray && parsed.isEmpty)
    }

    companion object {
        const val ACCOUNTS_PROPERTY = "alphatalk.price.accounts-json"
    }
}
