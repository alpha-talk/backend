package com.alphatalk.worker.batch

import com.alphatalk.worker.batch.master.StockMasterSyncJob
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertNotNull

@SpringBootTest(properties = ["alphatalk.batch.stock-master.cron=-"])
@Testcontainers(disabledWithoutDocker = true)
class BatchWorkerApplicationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val redis = GenericContainer("redis:7-alpine").withExposedPorts(6379)

        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var job: StockMasterSyncJob

    @Test
    fun `컨텍스트가 뜨고 마스터 동기화 잡이 조립된다`() {
        assertNotNull(job)
    }
}
