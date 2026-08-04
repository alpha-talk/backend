package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/db.changelog-worker-price-test.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaDailyCandleStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaDailyCandleStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
    }

    @Autowired
    private lateinit var store: JpaDailyCandleStore

    @Autowired
    private lateinit var repository: DailyCandleJpaRepository

    @Autowired
    private lateinit var entityManager: TestEntityManager

    private fun candle(date: String, close: Long = 71200) = KisDailyCandle(
        code = "005930",
        date = date,
        open = 70600,
        high = 71500,
        low = 70400,
        close = close,
        volume = 1234567,
        value = 87942671300,
    )

    private fun readBack() {
        entityManager.flush()
        entityManager.clear()
    }

    @Test
    fun `upsert는 두 번 실행해도 행 수가 같다`() {
        val candles = listOf(candle("20260724"), candle("20260723"))

        store.upsert(candles)
        store.upsert(candles)

        readBack()
        assertEquals(2, repository.count())
    }

    @Test
    fun `같은 날짜 재적재는 값을 덮어쓴다`() {
        store.upsert(listOf(candle("20260724", close = 71200)))
        store.upsert(listOf(candle("20260724", close = 71800)))

        readBack()
        val saved = repository.findById(DailyCandleId("005930", "20260724")).orElseThrow()
        assertEquals(71800, saved.close)
        assertEquals(87942671300, saved.tradedValue)
    }

    @Test
    fun `INTEGER 범위를 넘는 시세는 조용히 잘리지 않고 예외로 실패한다`() {
        assertFailsWith<ArithmeticException> {
            store.upsert(listOf(candle("20260724", close = Int.MAX_VALUE + 1L)))
        }
    }

    @Test
    fun `latestDate는 최신 일자를 반환하고 없으면 null이다`() {
        assertNull(store.latestDate("005930"))

        store.upsert(listOf(candle("20260723"), candle("20260724")))

        readBack()
        assertEquals("20260724", store.latestDate("005930"))
    }
}
