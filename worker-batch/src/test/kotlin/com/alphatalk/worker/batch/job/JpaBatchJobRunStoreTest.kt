package com.alphatalk.worker.batch.job

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.springframework.dao.DataIntegrityViolationException
import org.springframework.transaction.annotation.Propagation
import org.springframework.transaction.annotation.Transactional
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/db.changelog-master.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaBatchJobRunStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaBatchJobRunStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            org.testcontainers.utility.DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var runs: JpaBatchJobRunStore

    @Autowired
    private lateinit var repository: BatchJobRunJpaRepository

    @Test
    fun `같은 날 성공한 잡은 다시 시작되지 않는다`() {
        val first = runs.start("stock_master_sync", "20260729", Instant.now())
        assertNotNull(first)
        runs.succeed(first, 10, 0, Instant.now())

        assertNull(runs.start("stock_master_sync", "20260729", Instant.now()))
    }

    @Test
    fun `restart는 같은 날 SUCCESS여도 다시 시작한다 - 일내 반복 잡`() {
        val first = runs.restart("invest_opinion_sync", "20260807", Instant.now())
        runs.succeed(first, 5, 0, Instant.now())

        val second = runs.restart("invest_opinion_sync", "20260807", Instant.now())

        assertEquals(first, second)
        val row = repository.findById(second).orElseThrow()
        assertEquals("RUNNING", row.status)
        assertEquals(0, row.okCount)
        assertNull(row.finishedAt)
    }

    @Test
    fun `failCounted는 FAILED로 남기면서 성공·실패 카운트를 보존한다`() {
        val id = runs.start("valuation_daily", "20260807", Instant.now())
        assertNotNull(id)
        runs.failCounted(id, 2500, 100, "partial failure: failed=100", Instant.now())

        val row = repository.findById(id).orElseThrow()
        assertEquals("FAILED", row.status)
        assertEquals(2500, row.okCount)
        assertEquals(100, row.failCount)
        assertEquals("partial failure: failed=100", row.error)
        assertNotNull(runs.start("valuation_daily", "20260807", Instant.now()))
    }

    @Test
    fun `선행 성공 판정은 오늘 SUCCESS 행을 요구한다 - 행이 없으면 성공이 아니다`() {
        assertFalse(runs.hasSucceeded("stock_master_sync", "20260807"))

        val id = runs.start("stock_master_sync", "20260807", Instant.now())
        assertNotNull(id)
        assertFalse(runs.hasSucceeded("stock_master_sync", "20260807"))

        runs.failCounted(id, 3, 1, "partial universe", Instant.now())
        assertFalse(runs.hasSucceeded("stock_master_sync", "20260807"))

        runs.succeed(id, 3, 1, Instant.now())
        assertTrue(runs.hasSucceeded("stock_master_sync", "20260807"))
    }

    @Test
    fun `실패한 잡은 같은 날 다시 시작된다`() {
        val first = runs.start("stock_master_sync", "20260729", Instant.now())
        assertNotNull(first)
        runs.fail(first, "boom", Instant.now())

        val second = runs.start("stock_master_sync", "20260729", Instant.now())

        assertNotNull(second)
        assertEquals(first, second)
        assertEquals("RUNNING", repository.findById(second).orElseThrow().status)
    }

    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    fun `무결성 오류는 경합으로 오인되지 않고 전달된다`() {
        assertFailsWith<DataIntegrityViolationException> {
            runs.start("stock_master_sync", "202607290", Instant.now())
        }
    }

    @Test
    fun `잡 이력은 job과 run_date로 하나만 남는다`() {
        runs.start("stock_master_sync", "20260729", Instant.now())
        runs.start("stock_master_sync", "20260729", Instant.now())

        assertEquals(1, repository.count())
    }
}
