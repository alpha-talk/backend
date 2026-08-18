package com.alphatalk.worker.price

import com.alphatalk.worker.price.config.PriceProperties
import com.alphatalk.worker.price.session.PriceLifecycle
import com.alphatalk.worker.price.session.SessionPool
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
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class PriceWorkerApplicationTest {
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
    private lateinit var properties: PriceProperties

    @Autowired
    private lateinit var context: ApplicationContext

    @Test
    fun `컨텍스트 로드`() {
    }

    @Test
    fun `계정이 없으면 KIS 수집 평면이 뜨지 않는다`() {
        assertTrue(context.getBeanNamesForType(SessionPool::class.java).isEmpty())
        assertTrue(context.getBeanNamesForType(PriceLifecycle::class.java).isEmpty())
        assertEquals(200, properties.conflationMs)
    }
}
