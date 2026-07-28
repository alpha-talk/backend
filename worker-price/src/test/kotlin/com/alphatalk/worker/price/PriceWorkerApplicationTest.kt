package com.alphatalk.worker.price

import com.alphatalk.worker.price.config.PriceProperties
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@SpringBootTest(properties = ["alphatalk.price.enabled=false"])
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

    @Test
    fun `컨텍스트 로드`() {
    }

    @Test
    fun `기본 프로필은 비활성 상태로 뜨고 모의투자 환경을 가리킨다`() {
        assertFalse(properties.enabled)
        assertFalse(properties.candleEnabled)
        assertEquals("vts", properties.env)
        assertEquals(200, properties.conflationMs)
    }
}
