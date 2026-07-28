package com.alphatalk.worker.price.config

import com.alphatalk.worker.price.conflation.ConflationBuffer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PriceConfigTest {
    private val config = PriceConfig()
    private val validAccounts = """[{"keyId":"a1b2c3d4","appkey":"app-key","appsecret":"app-secret"}]"""

    private fun sessionPool(props: PriceProperties) =
        config.sessionPool(props, ConflationBuffer(), SimpleMeterRegistry())

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
    fun `종목이 비어 있으면 demandSource가 실패한다`() {
        assertFailsWith<IllegalStateException> {
            config.demandSource(PriceProperties(enabled = true, accountsJson = validAccounts, symbols = emptyList()))
        }
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
        assertNotNull(config.demandSource(props))
    }

    @Test
    fun `휴장일 설정이 캘린더로 파싱된다`() {
        val props = PriceProperties(holidays = listOf("2026-01-01", "2026-10-03"))

        assertNotNull(config.marketCalendar(props))
    }
}
