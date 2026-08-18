package com.alphatalk.worker.price.config

import org.mockito.Mockito
import org.springframework.context.annotation.ConditionContext
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.mock.env.MockEnvironment
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KisAccountsConditionTest {
    private val condition = KisAccountsCondition()

    private fun matches(accountsJson: String?): Boolean {
        val environment = MockEnvironment()
        accountsJson?.let { environment.setProperty(KisAccountsCondition.ACCOUNTS_PROPERTY, it) }
        val context = Mockito.mock(ConditionContext::class.java)
        Mockito.`when`(context.environment).thenReturn(environment)
        return condition.matches(context, Mockito.mock(AnnotatedTypeMetadata::class.java))
    }

    @Test
    fun `프로퍼티가 없으면 비활성`() {
        assertFalse(matches(null))
    }

    @Test
    fun `빈 문자열이나 빈 배열이면 비활성`() {
        assertFalse(matches(""))
        assertFalse(matches("  "))
        assertFalse(matches("[]"))
        assertFalse(matches(" [ ] "))
    }

    @Test
    fun `계정이 하나라도 있으면 활성`() {
        assertTrue(matches("""[{"keyId":"k","appkey":"a","appsecret":"s"}]"""))
    }

    @Test
    fun `깨진 JSON은 조용히 꺼지지 않고 활성으로 판정해 기동 실패로 이어진다`() {
        assertTrue(matches("{secret-blob"))
        assertTrue(matches("""{"keyId":"k"}"""))
    }
}
