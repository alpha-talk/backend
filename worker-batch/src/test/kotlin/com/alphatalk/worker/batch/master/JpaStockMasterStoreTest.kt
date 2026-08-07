package com.alphatalk.worker.batch.master

import com.alphatalk.kis.master.KisMarket
import com.alphatalk.kis.master.KisStockMaster
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
import java.time.LocalDate
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaStockMasterStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaStockMasterStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            org.testcontainers.utility.DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var store: JpaStockMasterStore


    @Autowired
    private lateinit var stocks: StockMasterJpaRepository


    @Autowired
    private lateinit var entityManager: TestEntityManager

    private fun stock(code: String, name: String = "종목$code", shares: Long? = 1_000L) = KisStockMaster(
        code = code,
        name = name,
        market = KisMarket.KOSPI,
        groupCode = "ST",
        sectorCode = "0027",
        sharesOutstanding = shares,
        listedAt = LocalDate.of(1975, 6, 11),
    )

    private fun readBack() {
        entityManager.flush()
        entityManager.clear()
    }

    @Test
    fun `upsert는 두 번 실행해도 행이 늘지 않고 값을 갱신한다`() {
        store.upsertAll(listOf(stock("005930", "삼성전자")))
        store.upsertAll(listOf(stock("005930", "삼성전자우")))

        readBack()
        assertEquals(1, stocks.count())
        assertEquals("삼성전자우", stocks.findById("005930").orElseThrow().name)
    }

    @Test
    fun `상장주수와 상장일이 없어도 적재된다`() {
        store.upsertAll(listOf(stock("000020", shares = null).copy(listedAt = null)))

        readBack()
        val saved = stocks.findById("000020").orElseThrow()
        assertNull(saved.sharesOutstanding)
        assertNull(saved.listedAt)
    }

    @Test
    fun `이번 동기화에 없는 종목만 비활성화된다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))

        val retired = store.deactivateMissing(listOf("005930"))

        assertEquals(1, retired)
        readBack()
        assertTrue(stocks.findById("005930").orElseThrow().isActive)
        assertFalse(stocks.findById("000660").orElseThrow().isActive)
    }

    @Test
    fun `수집 목록이 비면 아무것도 비활성화하지 않는다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))

        assertEquals(0, store.deactivateMissing(emptyList()))
        readBack()
        assertEquals(2, stocks.findAll().count { it.isActive })
    }

    @Test
    fun `상장폐지 후 재상장되면 다시 활성화된다`() {
        store.upsertAll(listOf(stock("005930"), stock("000660")))
        store.deactivateMissing(listOf("005930"))

        store.upsertAll(listOf(stock("000660")))

        readBack()
        assertTrue(stocks.findById("000660").orElseThrow().isActive)
    }

}
