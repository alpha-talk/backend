package com.alphatalk.worker.price.candle

import com.alphatalk.kis.rest.KisDailyCandle
import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Testcontainers(disabledWithoutDocker = true)
class JdbcDailyCandleStoreTest {
    companion object {
        private const val PRICE_CHANGELOG = "db/changelog/price/db.changelog-price.yaml"

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")

        private val jdbc by lazy {
            applyPriceChangelog()
            JdbcTemplate(DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password))
        }

        @Suppress("DEPRECATION")
        private fun applyPriceChangelog() {
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
                val database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(JdbcConnection(connection))
                Liquibase(PRICE_CHANGELOG, ClassLoaderResourceAccessor(), database)
                    .update(Contexts(), LabelExpression())
            }
        }
    }

    private val store by lazy { JdbcDailyCandleStore(jdbc) }

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

    @BeforeEach
    fun clean() {
        jdbc.update("DELETE FROM daily_candle")
    }

    @Test
    fun `upsert는 두 번 실행해도 행 수가 같다`() {
        val candles = listOf(candle("20260724"), candle("20260723"))

        store.upsert(candles)
        store.upsert(candles)

        assertEquals(2, jdbc.queryForObject("SELECT count(*) FROM daily_candle", Long::class.java))
    }

    @Test
    fun `같은 날짜 재적재는 값을 덮어쓴다`() {
        store.upsert(listOf(candle("20260724", close = 71200)))
        store.upsert(listOf(candle("20260724", close = 71800)))

        val close = jdbc.queryForObject(
            "SELECT close FROM daily_candle WHERE code = '005930' AND date = '20260724'",
            Long::class.java,
        )
        assertEquals(71800, close)
    }

    @Test
    fun `latestDate는 최신 일자를 반환하고 없으면 null이다`() {
        assertNull(store.latestDate("005930"))

        store.upsert(listOf(candle("20260723"), candle("20260724")))

        assertEquals("20260724", store.latestDate("005930"))
    }
}
