package com.alphatalk.worker.price.config

import com.alphatalk.kis.model.KisEnv
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

    private fun demandSource(props: PriceProperties) =
        config.demandSource(props, redisTemplate, connectionFactory)

    @Test
    fun `계정이 비어 있으면 기동에 실패한다`() {
        val e = assertFailsWith<IllegalStateException> {
            sessionPool(PriceProperties(enabled = true, accountsJson = "[]", symbols = listOf("005930")))
        }
        assertTrue("KIS_ACCOUNTS" in e.message.orEmpty())
    }

    @Test
    fun `계정 JSON이 깨져 있으면 원문 노출 없이 실패한다`() {
        val e = assertFailsWith<IllegalStateException> {
            sessionPool(PriceProperties(enabled = true, accountsJson = "{secret-blob", symbols = listOf("005930")))
        }
        assertTrue("파싱 실패" in e.message.orEmpty())
        assertTrue("secret-blob" !in e.message.orEmpty())
    }

    @Test
    fun `fixed 모드에서 종목이 비어 있으면 demandSource가 실패한다`() {
        assertFailsWith<IllegalStateException> {
            demandSource(PriceProperties(enabled = true, accountsJson = validAccounts, symbols = emptyList()))
        }
    }

    @Test
    fun `redis 모드는 종목 없이도 조립되고 fixed 모드 종목은 상시 유지분으로 남는다`() {
        val empty = PriceProperties(enabled = true, accountsJson = validAccounts, demandMode = DemandMode.REDIS)
        assertNotNull(demandSource(empty))

        val withBase = empty.copy(symbols = listOf("005930"))
        assertEquals(setOf("005930"), demandSource(withBase).targetSymbols())
    }

    @Test
    fun `지원하지 않는 env면 기동에 실패한다`() {
        assertFailsWith<IllegalArgumentException> {
            sessionPool(
                PriceProperties(
                    enabled = true,
                    env = "staging",
                    accountsJson = validAccounts,
                    symbols = listOf("005930"),
                ),
            )
        }
    }

    @Test
    fun `유효한 설정이면 풀과 수요 소스가 조립된다`() {
        val props = PriceProperties(enabled = true, accountsJson = validAccounts, symbols = listOf("005930"))

        assertNotNull(sessionPool(props))
        assertNotNull(demandSource(props))
    }

    @Test
    fun `실전은 통합·시간외 TR을, 모의는 KRX 정규장 TR만 구독한다`() {
        assertEquals(listOf("H0UNCNT0", "H0STOUP0"), config.tickTrIds(KisEnv.PROD))
        assertEquals(listOf("H0STCNT0"), config.tickTrIds(KisEnv.VTS))
    }

    @Test
    fun `휴장일 설정이 캘린더로 파싱된다`() {
        val props = PriceProperties(holidays = listOf("2026-01-01", "2026-10-03"))

        assertNotNull(config.marketCalendar(props))
    }
}
