package com.alphatalk.worker.price.candle

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

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/price/db.changelog-price.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaMinuteCandleStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaMinuteCandleStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
    }

    @Autowired
    private lateinit var store: JpaMinuteCandleStore

    @Autowired
    private lateinit var repository: MinuteCandleJpaRepository

    @Autowired
    private lateinit var entityManager: TestEntityManager

    private fun candle(time: String, date: String = "20260804", close: Long = 230500, value: Long = 100) =
        MinuteCandle(
            code = "005930",
            date = date,
            time = time,
            open = 230000,
            high = 230500,
            low = 229500,
            close = close,
            volume = 1000,
            value = value,
        )

    private fun readBack() {
        entityManager.flush()
        entityManager.clear()
    }

    @Test
    fun `upsert는 두 번 실행해도 행 수가 같다`() {
        store.upsert(listOf(candle("0900"), candle("0901")))
        readBack()
        store.upsert(listOf(candle("0900"), candle("0901")))
        readBack()

        assertEquals(2, repository.count())
    }

    @Test
    fun `같은 분 재적재는 값을 덮어쓴다`() {
        store.upsert(listOf(candle("0900", close = 230000)))
        readBack()
        store.upsert(listOf(candle("0900", close = 231000)))
        readBack()

        val row = repository.findAll().single()
        assertEquals(231000, row.close)
    }

    @Test
    fun `INTEGER 범위를 넘는 시세는 조용히 잘리지 않고 예외로 실패한다`() {
        assertFailsWith<ArithmeticException> {
            store.upsert(listOf(candle("0900", close = Long.MAX_VALUE)))
        }
    }

    @Test
    fun `latestTime은 해당 일자의 최신 분을 반환하고 없으면 null이다`() {
        store.upsert(listOf(candle("0900"), candle("0930"), candle("0910")))
        readBack()

        assertEquals("0930", store.latestTime("005930", "20260804"))
        assertNull(store.latestTime("005930", "20260803"))
        assertNull(store.latestTime("000660", "20260804"))
    }

    @Test
    fun `sumValueBefore는 기준 분 이전의 거래대금 합이다`() {
        store.upsert(listOf(candle("0900", value = 100), candle("0901", value = 200), candle("0902", value = 400)))
        readBack()

        assertEquals(300, store.sumValueBefore("005930", "20260804", "0902"))
        assertEquals(0, store.sumValueBefore("005930", "20260804", "0900"))
    }

    @Test
    fun `codesOn은 해당 일자에 행이 있는 종목 집합이다`() {
        store.upsert(listOf(candle("0900"), candle("0900", date = "20260803")))
        readBack()

        assertEquals(setOf("005930"), store.codesOn("20260804"))
        assertEquals(emptySet(), store.codesOn("20260801"))
    }

    @Test
    fun `purgeBefore는 기준일 이전 행을 지우고 개수를 돌려준다`() {
        store.upsert(
            listOf(candle("0900", date = "20260701"), candle("0900", date = "20260702"), candle("0900")),
        )
        readBack()

        assertEquals(2, store.purgeBefore("20260704"))
        readBack()
        assertEquals(1, repository.count())
    }
}
