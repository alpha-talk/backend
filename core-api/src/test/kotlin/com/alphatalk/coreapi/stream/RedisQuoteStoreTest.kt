package com.alphatalk.coreapi.stream

import com.alphatalk.contracts.Keys
import org.mockito.Mockito
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.core.HashOperations
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

class RedisQuoteStoreTest {
    private val redis = Mockito.mock(StringRedisTemplate::class.java)
    private val candles = Mockito.mock(DailyCandleJpaRepository::class.java)

    @Suppress("UNCHECKED_CAST")
    private val hashes = Mockito.mock(HashOperations::class.java) as HashOperations<String, String, String>

    private val seoul = ZoneId.of("Asia/Seoul")
    private val now = ZonedDateTime.of(2026, 8, 14, 12, 0, 0, 0, seoul).toInstant().toEpochMilli()
    private val store = RedisQuoteStore(
        redis,
        candles,
        Clock.fixed(Instant.ofEpochMilli(now), ZoneOffset.UTC),
    )

    private fun kstMillis(year: Int, month: Int, day: Int, hour: Int, minute: Int): Long =
        ZonedDateTime.of(year, month, day, hour, minute, 0, 0, seoul).toInstant().toEpochMilli()

    @BeforeTest
    fun setUp() {
        Mockito.`when`(redis.opsForHash<String, String>()).thenReturn(hashes)
    }

    private fun quoteHash(ts: Long) = mapOf(
        "price" to "71200",
        "prevClose" to "70500",
        "change" to "700",
        "changeRate" to "0.99",
        "open" to "70600",
        "high" to "71500",
        "low" to "70400",
        "volume" to "1234567",
        "ts" to ts.toString(),
    )

    @Test
    fun `Redis 조회 장애는 캐시 미스로 낮춰 일봉 폴백을 허용한다`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenThrow(RedisConnectionFailureException("redis unavailable"))

        assertNull(store.liveQuote("005930"))
    }

    @Test
    fun `깨진 시세 해시는 캐시 미스로 낮춘다`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenReturn(mapOf("price" to "not-a-number"))

        assertNull(store.liveQuote("005930"))
    }

    @Test
    fun `당일 캐시는 delayed false로 반환한다`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenReturn(quoteHash(ts = now - 1_000))

        val quote = store.liveQuote("005930")

        assertNotNull(quote)
        assertEquals(false, quote.delayed)
        assertEquals(71200L, quote.price)
    }

    @Test
    fun `당일 캐시는 몇 시간 조용해도 유효하다 - 체결 없는 종목의 마지막 체결가`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenReturn(quoteHash(ts = kstMillis(2026, 8, 14, 9, 5)))

        assertNotNull(store.liveQuote("005930"))
    }

    @Test
    fun `KST 자정 직후의 당일 캐시도 유효하다 - 날짜 경계`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenReturn(quoteHash(ts = kstMillis(2026, 8, 14, 0, 0)))

        assertNotNull(store.liveQuote("005930"))
    }

    @Test
    fun `이전 거래일의 캐시는 캐시 미스로 낮춰 일봉 폴백을 허용한다`() {
        Mockito.`when`(hashes.entries(Keys.price("005930")))
            .thenReturn(quoteHash(ts = kstMillis(2026, 8, 13, 23, 59)))

        assertNull(store.liveQuote("005930"))
    }

    @Test
    fun `일봉 폴백 시각은 해당 거래일 장 마감이다`() {
        Mockito.`when`(candles.findTop2ByCodeOrderByDateDesc("005930"))
            .thenReturn(
                listOf(
                    DailyCandleEntity(
                        code = "005930",
                        date = "20260728",
                        open = 70600,
                        high = 71500,
                        low = 70400,
                        close = 71200,
                        volume = 2000,
                    ),
                ),
            )

        val quote = store.lastCandleQuote("005930")
        val expected = LocalDate.of(2026, 7, 28)
            .atTime(15, 30)
            .atZone(ZoneId.of("Asia/Seoul"))
            .toInstant()
            .toEpochMilli()

        assertEquals(expected, quote?.ts)
    }
}
