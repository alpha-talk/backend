package com.alphatalk.coreapi.search

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.Liquibase
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.sql.DriverManager
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@Testcontainers(disabledWithoutDocker = true)
class JdbcStockSearchStoreTest {
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
        fun migrateAndSeed() {
            DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password).use { connection ->
                val database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(JdbcConnection(connection))
                Liquibase(MASTER_CHANGELOG, ClassLoaderResourceAccessor(), database)
                    .update(Contexts(), LabelExpression())
            }
            jdbc.update(
                """
                INSERT INTO stock_master (code, name, market, shares_outstanding, is_active) VALUES
                ('005930', '삼성전자',      'KOSPI',  5846278000, true),
                ('005935', '삼성전자우',    'KOSPI',   822886000, true),
                ('000660', 'SK하이닉스',    'KOSPI',   712702000, true),
                ('006400', '삼성SDI',       'KOSPI',    68764000, true),
                ('001234', '폐지된삼성',    'KOSPI',    10000000, false),
                ('035720', '카카오',        'KOSPI',  4432000000, true),
                ('000440', '중앙에너비스',  'KOSDAQ',    6227000, true)
                """.trimIndent(),
            )
        }
    }

    private val store by lazy { JdbcStockSearchStore(jdbc) }

    @Test
    fun `이름 일부로 찾는다`() {
        val found = store.search("삼성", 10)

        assertEquals(listOf("005930", "005935", "006400"), found.map { it.code })
    }

    @Test
    fun `코드 앞자리로 찾는다`() {
        val found = store.search("0059", 10)

        assertEquals(listOf("005930", "005935"), found.map { it.code })
    }

    @Test
    fun `코드 일치가 이름 일치보다 앞선다`() {
        val found = store.search("000660", 10)

        assertEquals("000660", found.first().code)
    }

    @Test
    fun `규모가 큰 종목이 앞에 온다`() {
        val found = store.search("삼성", 10)

        val shares = found.map { it.code }
        assertEquals("005930", shares.first())
        assertTrue(shares.indexOf("005935") < shares.indexOf("006400"))
    }

    @Test
    fun `상장폐지 종목은 결과에서 빠진다`() {
        val found = store.search("삼성", 30)

        assertTrue(found.none { it.code == "001234" }, "비활성 종목이 검색됐다")
    }

    @Test
    fun `limit만큼만 돌려준다`() {
        assertEquals(1, store.search("삼성", 1).size)
        assertEquals(3, store.search("삼성", 30).size)
    }

    @Test
    fun `시장 구분을 함께 돌려준다`() {
        assertEquals("KOSPI", store.search("005930", 10).single().market)
        assertEquals("KOSDAQ", store.search("중앙", 10).single().market)
    }

    @Test
    fun `LIKE 특수문자는 검색어로만 쓰인다`() {
        val percent = store.search("%", 10)
        val underscore = store.search("_", 10)

        assertTrue(percent.isEmpty(), "%가 와일드카드로 동작했다")
        assertTrue(underscore.isEmpty(), "_가 와일드카드로 동작했다")
    }

    @Test
    fun `이름 검색에 trgm 인덱스가 걸려 있다`() {
        val indexes = jdbc.queryForList(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'stock_master'",
            String::class.java,
        )

        assertTrue("idx_stock_master_name_trgm" in indexes, "trgm 인덱스 없음: $indexes")
    }
}
