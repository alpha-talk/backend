package com.alphatalk.coreapi.search

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@DataJpaTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaStockSearchStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaStockSearchStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var store: JpaStockSearchStore

    @Autowired
    private lateinit var jdbc: JdbcTemplate

    @BeforeEach
    fun seed() {
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
        jdbc.update(
            """
            INSERT INTO stock_master (code, name, market, shares_outstanding, is_active)
            SELECT lpad((100000 + g)::text, 6, '0'), '가상종목' || g, 'KOSDAQ', g * 1000, true
            FROM generate_series(1, 2600) g
            """.trimIndent(),
        )
        jdbc.execute("ANALYZE stock_master")
    }

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

    @Test
    fun `코드 인덱스는 LIKE 접두어 검색에 실제로 쓰인다`() {
        jdbc.execute("SET enable_seqscan = off")
        try {
            val plan = jdbc.queryForList(
                "EXPLAIN SELECT code FROM stock_master WHERE is_active AND code LIKE '0059%'",
                String::class.java,
            ).joinToString("\n")

            assertTrue("idx_stock_master_active_code" in plan, "인덱스가 계획에 없다:\n$plan")
        } finally {
            jdbc.execute("SET enable_seqscan = on")
        }
    }

    @Test
    fun `실제 검색 쿼리 모양은 순차 스캔 없이 코드 인덱스로 실행된다`() {
        val namePlan = searchPlan(prefix = "삼성%", contains = "%삼성%")
        val codePlan = searchPlan(prefix = "0059%", contains = "%0059%")

        assertTrue("Seq Scan" !in namePlan, "이름 검색이 순차 스캔이다:\n$namePlan")
        assertTrue("Seq Scan" !in codePlan, "코드 검색이 순차 스캔이다:\n$codePlan")
        assertTrue("idx_stock_master_active_code" in codePlan, "코드 인덱스가 계획에 없다:\n$codePlan")
    }

    private fun searchPlan(prefix: String, contains: String): String {
        jdbc.execute("SET enable_seqscan = off")
        try {
            return jdbc.queryForList(
                """
                EXPLAIN SELECT trim(BOTH FROM s.code), s.name, s.market
                FROM stock_master s
                WHERE s.is_active = true
                  AND (s.code LIKE '$prefix' ESCAPE '!' OR s.name ILIKE '$contains' ESCAPE '!')
                ORDER BY
                    CASE
                        WHEN s.code LIKE '$prefix' ESCAPE '!' THEN 0
                        WHEN s.name ILIKE '$prefix' ESCAPE '!' THEN 1
                        ELSE 2
                    END,
                    s.shares_outstanding DESC NULLS LAST,
                    s.code
                FETCH FIRST 10 ROWS ONLY
                """.trimIndent(),
                String::class.java,
            ).joinToString("\n")
        } finally {
            jdbc.execute("SET enable_seqscan = on")
        }
    }

    @Test
    fun `코드 인덱스는 패턴 검색용 opclass를 쓴다`() {
        val definition = jdbc.queryForObject(
            "SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_stock_master_active_code'",
            String::class.java,
        )

        assertTrue(
            definition!!.contains("bpchar_pattern_ops"),
            "기본 콜레이션에서 LIKE에 쓰이지 않는 opclass다: $definition",
        )
    }
}
