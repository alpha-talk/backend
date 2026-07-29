package com.alphatalk.coreapi.subscription

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class JdbcWatchlistStoreTest {
    companion object {
        private const val MASTER_CHANGELOG = "db/changelog/db.changelog-master.yaml"

        @Container
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )

        private val jdbc by lazy {
            JdbcTemplate(DriverManagerDataSource(postgres.jdbcUrl, postgres.username, postgres.password))
        }

        @Suppress("DEPRECATION")
        @BeforeAll
        @JvmStatic
        fun migrate() {
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
                val database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(JdbcConnection(connection))
                Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database)
                    .update(Contexts(), LabelExpression())
            }
        }
    }

    private val store by lazy { JdbcWatchlistStore(jdbc) }
    private val catalog by lazy { JdbcStockCatalog(jdbc) }
    private var userId = 0L

    @BeforeEach
    fun seed() {
        jdbc.update("DELETE FROM watchlist")
        jdbc.update("DELETE FROM users")
        jdbc.update("DELETE FROM stock_master")
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
            ('005930', '삼성전자',   'KOSPI',  5846278000, true),
            ('000660', 'SK하이닉스', 'KOSPI',   712702000, true),
            ('000440', '중앙에너비스', 'KOSDAQ',   6227000, true),
            ('001234', '폐지된종목', 'KOSPI',    10000000, false)
            """.trimIndent(),
        )
        userId = jdbc.queryForObject(
            "INSERT INTO users (email, password_hash, nickname) VALUES ('a@b.c', 'x', '민균') RETURNING id",
            Long::class.java,
        )!!
    }

    @Test
    fun `담은 종목을 종목명과 함께 돌려준다`() {
        store.add(userId, "005930")

        val item = store.list(userId).single()
        assertEquals("005930", item.code)
        assertEquals("삼성전자", item.name)
        assertEquals("KOSPI", item.market)
        assertTrue(item.subscribedAt > 0)
    }

    @Test
    fun `최근에 담은 종목이 앞에 온다`() {
        store.add(userId, "005930")
        Thread.sleep(5)
        store.add(userId, "000660")

        assertEquals(listOf("000660", "005930"), store.list(userId).map { it.code })
    }

    @Test
    fun `같은 종목을 두 번 담아도 한 건이다`() {
        assertTrue(store.add(userId, "005930"))
        assertFalse(store.add(userId, "005930"))

        assertEquals(1, store.list(userId).size)
    }

    @Test
    fun `해지하면 목록에서 빠진다`() {
        store.add(userId, "005930")

        assertTrue(store.remove(userId, "005930"))
        assertFalse(store.remove(userId, "005930"))
        assertTrue(store.list(userId).isEmpty())
    }

    @Test
    fun `상태는 보유 건수와 구독 여부를 한 번에 알려준다`() {
        store.add(userId, "005930")
        store.add(userId, "000660")

        assertEquals(WatchlistState(2, true), store.state(userId, "005930"))
        assertEquals(WatchlistState(2, false), store.state(userId, "000440"))
    }

    @Test
    fun `다른 사용자의 관심목록은 섞이지 않는다`() {
        val other = jdbc.queryForObject(
            "INSERT INTO users (email, password_hash, nickname) VALUES ('x@y.z', 'x', '다른사람') RETURNING id",
            Long::class.java,
        )!!
        store.add(userId, "005930")
        store.add(other, "000660")

        assertEquals(listOf("005930"), store.list(userId).map { it.code })
        assertEquals(WatchlistState(1, false), store.state(userId, "000660"))
    }

    @Test
    fun `탈퇴하면 관심목록도 함께 지워진다`() {
        store.add(userId, "005930")

        jdbc.update("DELETE FROM users WHERE id = ?", userId)

        assertEquals(0, jdbc.queryForObject("SELECT count(*) FROM watchlist", Int::class.java))
    }

    @Test
    fun `종목 존재 확인은 상장된 종목만 인정한다`() {
        assertTrue(catalog.exists("005930"))
        assertFalse(catalog.exists("001234"), "상장폐지 종목이 통과했다")
        assertFalse(catalog.exists("999999"))
    }
}
