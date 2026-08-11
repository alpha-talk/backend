package com.alphatalk.worker.price.config

import com.alphatalk.contracts.DemandTiming
import com.alphatalk.worker.price.conflation.ConflationBuffer
import com.alphatalk.worker.price.market.InMemoryMarketDivStore
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
        config.sessionPool(props, ConflationBuffer(), SimpleMeterRegistry(), InMemoryMarketDivStore())

    private fun demandSource(props: PriceProperties = PriceProperties()) =
        config.demandSource(props, redisTemplate, connectionFactory)

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
    fun `기본 설정은 게이트웨이 재등록 지연을 더해도 구독 해제 유예 안에 복구된다`() {
        val props = PriceProperties()

        assertTrue(DemandTiming.GATEWAY_HEARTBEAT_SECONDS * 1_000 + props.demandReconcileMs < props.removalGraceMs)
    }

    @Test
    fun `재등록 지연을 더한 복구 시간이 구독 해제 유예를 넘으면 기동에 실패한다`() {
        assertFailsWith<IllegalArgumentException> {
            PriceProperties(removalGraceMs = 30_000, demandReconcileMs = 30_000)
        }
        assertFailsWith<IllegalArgumentException> {
            PriceProperties(removalGraceMs = 2_000, demandReconcileMs = 1_000)
        }
        assertFailsWith<IllegalArgumentException> {
            PriceProperties(removalGraceMs = 30_000, demandReconcileMs = 0)
        }
        assertFailsWith<IllegalArgumentException> {
            PriceProperties(removalGraceMs = 30_000, demandReconcileMs = Long.MAX_VALUE)
        }
        assertFailsWith<IllegalArgumentException> {
            PriceProperties(removalGraceMs = Long.MIN_VALUE, demandReconcileMs = 10_000)
        }
    }

    @Test
    fun `conflation 주기가 명세 범위를 벗어나면 기동에 실패한다`() {
        assertFailsWith<IllegalArgumentException> { PriceProperties(conflationMs = 0) }
        assertFailsWith<IllegalArgumentException> { PriceProperties(conflationMs = 99) }
        assertFailsWith<IllegalArgumentException> { PriceProperties(conflationMs = 251) }
        assertEquals(200, PriceProperties().conflationMs)
    }

    @Test
    fun `틱 구독 기본은 통합·시간외이고 KRX 전용은 종목별로 대체된다`() {
        assertEquals(listOf("H0UNCNT0", "H0STOUP0"), PriceConfig.TICK_TR_IDS)
    }

    @Test
    fun `휴장일 설정이 캘린더로 파싱된다`() {
        val props = PriceProperties(holidays = listOf("2026-01-01", "2026-10-03"))

        assertNotNull(config.marketCalendar(props))
    }
}
