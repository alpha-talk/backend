package com.alphatalk.worker.price.config

import com.alphatalk.worker.price.conflation.ConflationBuffer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.StringRedisTemplate
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PriceConfigTest {
    private val config = PriceConfig()
    private val validAccounts = """[{"keyId":"a1b2c3d4","appkey":"app-key","appsecret":"app-secret"}]"""
    private val connectionFactory = LettuceConnectionFactory()
    private val redisTemplate = StringRedisTemplate()

    private fun sessionPool(props: PriceProperties) =
        config.sessionPool(props, ConflationBuffer(), SimpleMeterRegistry())

    private fun demandSource() = config.demandSource(redisTemplate, connectionFactory)

    @Test
    fun `계정이 비어 있으면 기동에 실패한다`() {
        val e = assertFailsWith<IllegalStateException> {
            sessionPool(PriceProperties(enabled = true, accountsJson = "[]"))
        }
        assertTrue("KIS_ACCOUNTS" in e.message.orEmpty())
    }

    @Test
    fun `계정 JSON이 깨져 있으면 원문 노출 없이 실패한다`() {
        val e = assertFailsWith<IllegalStateException> {
            sessionPool(PriceProperties(enabled = true, accountsJson = "{secret-blob"))
        }
        assertTrue("파싱 실패" in e.message.orEmpty())
        assertTrue("secret-blob" !in e.message.orEmpty())
    }

    @Test
    fun `수요 소스는 게이트웨이 수요만 사용한다 - 초기 목표 집합은 공집합`() {
        assertEquals(emptySet(), demandSource().targetSymbols())
    }

    @Test
    fun `유효한 설정이면 풀과 수요 소스가 조립된다`() {
        val props = PriceProperties(enabled = true, accountsJson = validAccounts)

        assertNotNull(sessionPool(props))
        assertNotNull(demandSource())
    }

    @Test
    fun `틱 구독은 통합·시간외 TR만 쓴다 - KRX 전용 TR은 NXT 체결분이 빠진다`() {
        assertEquals(listOf("H0UNCNT0", "H0STOUP0"), PriceConfig.TICK_TR_IDS)
    }

    @Test
    fun `휴장일 설정이 캘린더로 파싱된다`() {
        val props = PriceProperties(holidays = listOf("2026-01-01", "2026-10-03"))

        assertNotNull(config.marketCalendar(props))
    }
}
