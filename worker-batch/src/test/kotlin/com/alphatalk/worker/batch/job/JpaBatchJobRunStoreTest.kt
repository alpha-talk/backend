package com.alphatalk.worker.batch.job

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.annotation.Import
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@DataJpaTest(properties = ["spring.liquibase.change-log=classpath:db/changelog/batch/db.changelog-batch.yaml"])
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaBatchJobRunStore::class)
@Testcontainers(disabledWithoutDocker = true)
class JpaBatchJobRunStoreTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer("postgres:16-alpine")
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
    fun `잡 이력은 job과 run_date로 하나만 남는다`() {
        runs.start("stock_master_sync", "20260729", Instant.now())
        runs.start("stock_master_sync", "20260729", Instant.now())

        assertEquals(1, repository.count())
    }
}
