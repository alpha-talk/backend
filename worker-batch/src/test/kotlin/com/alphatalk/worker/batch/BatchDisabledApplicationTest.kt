package com.alphatalk.worker.batch

import com.alphatalk.worker.batch.stockinfo.InvestorFlowSyncJob
import com.alphatalk.worker.batch.stockinfo.ValuationSyncJob
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.ApplicationContext
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "alphatalk.batch.enabled=false",
        "alphatalk.batch.opinion.enabled=true",
        "alphatalk.batch.investor.enabled=true",
        "alphatalk.batch.kis.accounts-json=[]",
    ],
)
@Testcontainers(disabledWithoutDocker = true)
class BatchDisabledApplicationTest {
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
    private lateinit var context: ApplicationContext

    @Test
    fun `배치를 끈 환경은 KIS 계정 없이도 기동하고 켜진 KIS 잡도 조립되지 않는다`() {
        assertTrue(context.getBeansOfType(ValuationSyncJob::class.java).isEmpty())
        assertTrue(context.getBeansOfType(InvestorFlowSyncJob::class.java).isEmpty())
    }
}
