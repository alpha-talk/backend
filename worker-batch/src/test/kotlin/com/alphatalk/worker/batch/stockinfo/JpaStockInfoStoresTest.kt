package com.alphatalk.worker.batch.stockinfo

import com.alphatalk.worker.batch.financials.FinancialFigures
import com.alphatalk.worker.batch.financials.FinancialSummaryId
import com.alphatalk.worker.batch.financials.FinancialSummaryJpaRepository
import com.alphatalk.worker.batch.financials.FinancialSummaryRow
import com.alphatalk.worker.batch.financials.JpaFinancialSummaryStore
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.math.BigDecimal
import java.time.Instant
import kotlin.test.assertEquals

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaValuationStore::class, JpaInvestorFlowStore::class, JpaFinancialSummaryStore::class, JpaStockUniverse::class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers(disabledWithoutDocker = true)
class JpaStockInfoStoresTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var valuationStore: JpaValuationStore

    @Autowired
    private lateinit var valuations: ValuationDailyJpaRepository

    @Autowired
    private lateinit var investorStore: JpaInvestorFlowStore

    @Autowired
    private lateinit var flows: InvestorFlowJpaRepository

    @Autowired
    private lateinit var financialStore: JpaFinancialSummaryStore

    @Autowired
    private lateinit var financials: FinancialSummaryJpaRepository

    @AfterEach
    fun cleanUp() {
        valuations.deleteAll()
        flows.deleteAll()
        financials.deleteAll()
    }

    @Test
    fun `밸류에이션 업서트는 삽입과 갱신에 수렴한다`() {
        val first = ValuationRow("005930", "20260807", BigDecimal("12.10"), BigDecimal("1.35"), 5771, 52002, 425_000_000_000_000)
        valuationStore.upsert(listOf(first))
        valuationStore.upsert(listOf(first.copy(per = BigDecimal("12.34"), marketCap = 430_000_000_000_000)))

        val row = valuations.findAll().single()
        assertEquals(0, BigDecimal("12.34").compareTo(row.per))
        assertEquals(430_000_000_000_000, row.marketCap)
    }

    @Test
    fun `투자자 순매수 업서트는 삽입과 갱신에 수렴한다`() {
        val first = InvestorFlowRow("005930", "20260807", -12000, 8000, 4000)
        investorStore.upsert(listOf(first, InvestorFlowRow("005930", "20260806", 1, 2, 3)))
        investorStore.upsert(listOf(first.copy(individual = -13000)))

        assertEquals(2, flows.count())
        val row = flows.findById(InvestorFlowId("005930", "20260807")).orElseThrow()
        assertEquals(-13000, row.individual)
        assertEquals(8000, row.foreign)
    }

    @Test
    fun `재무 요약 업서트는 삽입과 갱신에 수렴한다`() {
        val row = FinancialSummaryRow(
            code = "005930",
            year = 2026,
            reprtCode = "11012",
            fsDiv = "CFS",
            figures = FinancialFigures(3000, 350, 280, 4000, 1500, 2500),
            disclosedAt = Instant.parse("2026-08-04T15:00:00Z"),
        )
        financialStore.upsert(row)
        financialStore.upsert(row.copy(figures = FinancialFigures(3100, 350, 280, 4000, 1500, 2500), fsDiv = "OFS"))

        val stored = financials.findById(FinancialSummaryId("005930", 2026, "11012")).orElseThrow()
        assertEquals(3100, stored.revenue)
        assertEquals("OFS", stored.fsDiv)
    }
}
