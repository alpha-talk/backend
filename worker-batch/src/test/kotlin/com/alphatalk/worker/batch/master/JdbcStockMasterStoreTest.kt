package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisStockMaster
import com.alphatalk.worker.batch.job.JdbcBatchJobRunStore
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
import java.time.Instant
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class JdbcStockMasterStoreTest {
    companion object {
        private const val BATCH_CHANGELOG = "db/changelog/batch/db.changelog-batch.yaml"

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")

        private val jdbc by lazy {
            applyChangelog()
            JdbcTemplate(DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password))
        }

        @Suppress("DEPRECATION")
        private fun applyChangelog() {
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
                val database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(JdbcConnection(connection))
                Liquibase(BATCH_CHANGELOG, ClassLoaderResourceAccessor(), database)
                    .update(Contexts(), LabelExpression())
            }
        }
    }

    private val store by lazy { JdbcStockMasterStore(jdbc) }
    private val runs by lazy { JdbcBatchJobRunStore(jdbc) }

    private fun stock(code: String, name: String = "종목$code", shares: Long? = 1_000L) = KisStockMaster(
        code = code,
        name = name,
        market = KisMarket.KOSPI,
        groupCode = "ST",
        sectorCode = "0027",
        sharesOutstanding = shares,
        listedAt = LocalDate.of(1975, 6, 11),
    )

    @BeforeEach
    fun clean() {
        jdbc.update("DELETE FROM stock_master")
        jdbc.update("DELETE FROM batch_job_run")
    }

    @Test
    fun `upsert는 두 번 실행해도 행이 늘지 않고 값을 갱신한다`() {
        store.upsertAll(listOf(stock("005930", "삼성전자")))
        store.upsertAll(listOf(stock("005930", "삼성전자우")))

        assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM stock_master", Long::class.java)?.toInt())
        assertEquals(
            "삼성전자우",
            jdbc.queryForObject("SELECT name FROM stock_master WHERE code = '005930'", String::class.java),
        )
    }

    @Test
    fun `상장주수와 상장일이 없어도 적재된다`() {
        store.upsertAll(listOf(stock("000020", shares = null).copy(listedAt = null)))

        assertNull(jdbc.queryForObject("SELECT shares_outstanding FROM stock_master WHERE code='000020'", Long::class.java))
        assertNull(jdbc.queryForObject("SELECT listed_at FROM stock_master WHERE code='000020'", java.sql.Date::class.java))
    }

    @Test
    fun `이번 동기화에 없는 종목만 비활성화된다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))

        val retired = store.deactivateMissing(listOf("005930"))

        assertEquals(1, retired)
        assertEquals(true, jdbc.queryForObject("SELECT is_active FROM stock_master WHERE code='005930'", Boolean::class.java))
        assertEquals(false, jdbc.queryForObject("SELECT is_active FROM stock_master WHERE code='000660'", Boolean::class.java))
    }

    @Test
    fun `수집 목록이 비면 아무것도 비활성화하지 않는다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))

        assertEquals(0, store.deactivateMissing(emptyList()))
        assertEquals(
            2,
            jdbc.queryForObject("SELECT count(*) FROM stock_master WHERE is_active = true", Long::class.java)?.toInt(),
        )
    }

    @Test
    fun `상장폐지 후 재상장되면 다시 활성화된다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))
        store.deactivateMissing(listOf("005930"))

        store.upsertAll(listOf(stock("000660")))

        assertEquals(true, jdbc.queryForObject("SELECT is_active FROM stock_master WHERE code='000660'", Boolean::class.java))
    }

    @Test
    fun `같은 날 성공한 잡은 다시 시작되지 않는다`() {
        val first = runs.start("stock_master_sync", "20260729", Instant.now())
        assertNotNull(first)
        runs.succeed(first, 10, 0, Instant.now())

        assertNull(runs.start("stock_master_sync", "20260729", Instant.now()))
    }

    @Test
    fun `실패한 잡은 같은 날 다시 시작된다`() {
        val first = runs.start("stock_master_sync", "20260729", Instant.now())
        assertNotNull(first)
        runs.fail(first, "boom", Instant.now())

        val second = runs.start("stock_master_sync", "20260729", Instant.now())

        assertNotNull(second)
        assertEquals(first, second)
        assertEquals(
            "RUNNING",
            jdbc.queryForObject("SELECT status FROM batch_job_run WHERE id = ?", String::class.java, second),
        )
    }

    @Test
    fun `잡 이력은 job과 run_date로 하나만 남는다`() {
        runs.start("stock_master_sync", "20260729", Instant.now())
        runs.start("stock_master_sync", "20260729", Instant.now())

        assertTrue(jdbc.queryForObject("SELECT count(*) FROM batch_job_run", Long::class.java) == 1L)
    }
}
