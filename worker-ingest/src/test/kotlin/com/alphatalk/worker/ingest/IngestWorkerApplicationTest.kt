package com.alphatalk.worker.ingest

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.inbox.IngestInbox
import com.alphatalk.worker.ingest.inbox.IngestInboxRepository
import com.alphatalk.worker.ingest.scheduler.DigestUniverse
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.scheduler.MarketDigestTrigger
import com.alphatalk.worker.ingest.source.FeedValidators
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.transaction.support.TransactionTemplate
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull

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

    @Autowired
    private lateinit var inbox: IngestInbox

    @Autowired
    private lateinit var articles: IngestInboxRepository

    @Autowired
    private lateinit var transactions: TransactionTemplate

    @Test
    fun `피드 검증 값과 기사는 함께 커밋되고 함께 롤백된다`() {
        val article = IngestQueueEntry(
            source = "test",
            sourceId = "test:atomic-capture",
            type = IngestType.NEWS,
            codes = emptyList(),
            title = "테스트 기사",
            url = "https://example.com/article",
            fetchedAt = 1_700_000_000_000,
        )
        val feedId = "atomic-capture"
        val feedUrl = "https://example.com/rss"
        val validators = FeedValidators("\"v1\"", 1_700_000_000_000)

        transactions.executeWithoutResult { status ->
            inbox.capture(feedId, feedUrl, null, validators, listOf(article))
            status.setRollbackOnly()
        }
        assertNull(inbox.checkpoint(feedId, feedUrl))
        assertFalse(articles.existsById(article.sourceId))

        assertEquals(1, inbox.capture(feedId, feedUrl, null, validators, listOf(article)))
        assertEquals(validators, inbox.checkpoint(feedId, feedUrl)?.validators)
        assertEquals(FeedValidators(), inbox.checkpoint(feedId, "https://example.com/new-rss")?.validators)
        assertEquals(0, inbox.capture(feedId, feedUrl, inbox.checkpoint(feedId, feedUrl), validators, listOf(article)))
        assertNotNull(articles.findById(article.sourceId).orElse(null))
    }

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
