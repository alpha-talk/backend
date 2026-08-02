package com.alphatalk.coreapi.stream

import com.alphatalk.contracts.Keys
import org.mockito.Mockito
import org.springframework.data.redis.RedisConnectionFailureException
import org.springframework.data.redis.core.HashOperations
import org.springframework.data.redis.core.StringRedisTemplate
import java.time.LocalDate
import java.time.ZoneId
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class RedisQuoteStoreTest {
    private val redis = Mockito.mock(StringRedisTemplate::class.java)
    private val candles = Mockito.mock(DailyCandleJpaRepository::class.java)

    @Suppress("UNCHECKED_CAST")
    private val hashes = Mockito.mock(HashOperations::class.java) as HashOperations<String, String, String>

    private val store = RedisQuoteStore(redis, candles)

    @BeforeTest
    fun setUp() {
        Mockito.`when`(redis.opsForHash<String, String>()).thenReturn(hashes)
    }

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
