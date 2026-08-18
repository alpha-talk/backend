package com.alphatalk.worker.price

import com.alphatalk.worker.price.candle.CandleSyncJob
import com.alphatalk.worker.price.candle.MinuteCandleDailySyncJob
import com.alphatalk.worker.price.candle.MinuteCandleRefreshController
import com.alphatalk.worker.price.poll.RestPollingScheduler
import com.alphatalk.worker.price.session.PriceLifecycle
import com.alphatalk.worker.price.session.SessionPool
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.context.ApplicationContext
import org.springframework.test.annotation.DirtiesContext
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@SpringBootTest(
    properties = [
        "alphatalk.price.accounts-json=[{\"keyId\":\"test-key\",\"appkey\":\"test-app\",\"appsecret\":\"test-secret\"}]",
    ],
)
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class PriceWorkerActivationTest {
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
    fun `계정이 설정되면 실시간·일봉·분봉 수집 평면이 전부 조립된다`() {
        assertNotNull(context.getBean(SessionPool::class.java))
        assertNotNull(context.getBean(PriceLifecycle::class.java))
        assertNotNull(context.getBean(RestPollingScheduler::class.java))
        assertNotNull(context.getBean(CandleSyncJob::class.java))
        assertNotNull(context.getBean(MinuteCandleDailySyncJob::class.java))
        assertNotNull(context.getBean(MinuteCandleRefreshController::class.java))
    }

    @Test
    fun `일봉 기동 백필은 별도 opt-in 없이는 뜨지 않는다`() {
        assertTrue(!context.containsBean("candleStartupSync"))
    }
}
