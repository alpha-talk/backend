package com.alphatalk.worker.ingest

import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.scheduler.DigestUniverse
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.scheduler.MarketDigestTrigger
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@SpringBootTest(
    properties = [
        "alphatalk.ingest.poll-enabled=false",
        "alphatalk.ingest.digest.catch-up-on-startup=false",
        "alphatalk.ingest.digest.cron=-",
        "alphatalk.ingest.digest.market-cron=-",
    ],
)
@Testcontainers(disabledWithoutDocker = true)
class IngestWorkerApplicationTest {
    companion object {
        @Container
        @ServiceConnection
        @JvmStatic
        val postgres = PostgreSQLContainer(
            DockerImageName.parse("pgvector/pgvector:pg16").asCompatibleSubstituteFor("postgres"),
        )
    }

    @Autowired
    private lateinit var properties: IngestProperties

    @Autowired
    private lateinit var digestUniverse: DigestUniverse

    @Autowired
    private lateinit var poller: IngestPoller

    @Autowired
    private lateinit var newsSources: List<NewsSource>

    @Autowired
    private lateinit var marketDigestTrigger: MarketDigestTrigger

    @Test
    fun `컨텍스트 로드`() {
    }

    @Test
    fun `폴러는 컴포넌트 스캔으로 등록되고 설정된 뉴스 소스를 모두 주입받는다`() {
        assertNotNull(poller)
        assertEquals(properties.feeds.size, newsSources.size)
        assertEquals(properties.feeds.map { it.id }, newsSources.map { it.id })
    }

    @Test
    fun `시장 다이제스트 트리거는 기본 프로필에서 활성화된다`() {
        assertNotNull(marketDigestTrigger)
        assertEquals(true, properties.digest.marketEnabled)
    }

    @Test
    fun `다이제스트 대상은 watchlist 유니버스가 소유하고 빈 DB에서는 빈 목록이다`() {
        assertEquals(emptyList(), digestUniverse.codes())
        assertEquals(1, properties.digest.enqueueConcurrency)
    }

    @Test
    fun `기본 프로필에 주식 관련 RSS 피드만 등록된다`() {
        assertEquals(6, properties.feeds.size)
        assertEquals(
            setOf("yna", "hankyung", "donga", "chosun"),
            properties.feeds.map { it.source }.toSet(),
        )
        assertEquals(properties.feeds.size, properties.feeds.map { it.id }.distinct().size)
        assertFalse(properties.feeds.any { "society" in it.id })
        assertFalse(properties.feeds.any { "politics" in it.id })
    }
}
