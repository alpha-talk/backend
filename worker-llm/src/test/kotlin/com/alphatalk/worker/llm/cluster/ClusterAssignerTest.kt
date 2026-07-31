package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import com.alphatalk.worker.llm.config.LlmProperties
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ClusterAssignerTest {
    private val store = InMemoryClusterStore()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T09:00:00Z"), ZoneOffset.UTC)
    private val clusterProps = LlmProperties(
        cluster = LlmProperties.Cluster(windowHours = 72, similarityThreshold = 0.85),
    )
    private var idCounter = 0
    private val assigner = ClusterAssigner(
        store = store,
        embeddings = FakeEmbeddingClient(4096),
        lock = NoopClusterLock(),
        props = clusterProps,
        clusterIds = { "cluster-${++idCounter}".padEnd(26, '0') },
        clock = clock,
    )

    private fun entry(
        sourceId: String,
        title: String,
        source: String = "hankyung",
        codes: List<String> = listOf("005930"),
        macroHint: String? = null,
    ) = IngestQueueEntry(
        source = source,
        sourceId = sourceId,
        type = IngestType.NEWS,
        codes = codes,
        title = title,
        url = "https://example.com/$sourceId",
        fetchedAt = clock.millis(),
        macroHint = macroHint,
    )

    @Test
    fun `첫 기사 - 신규 클러스터 생성`() {
        val result = assigner.assign(entry("a1", "삼성전자 3나노 수주"))
        assertTrue(result.created)
        assertFalse(result.joined)
        assertEquals(1, store.clusters.size)
    }

    @Test
    fun `정규화 제목이 같으면 임베딩 없이 편입`() {
        val first = assigner.assign(entry("a1", "삼성전자 3나노 수주"))
        val second = assigner.assign(entry("b1", "[속보] 삼성전자, 3나노 수주!", source = "maeil"))
        assertTrue(second.joined)
        assertEquals(first.clusterId, second.clusterId)
        assertEquals(1, store.clusters.size)
    }

    @Test
    fun `임베딩 유사도 임계 이상이면 편입`() {
        val first = assigner.assign(entry("a1", "삼성전자 3나노 파운드리 대규모 수주 계약 공시"))
        val second = assigner.assign(entry("b1", "삼성전자 3나노 파운드리 대규모 수주 계약 발표", source = "maeil"))
        assertTrue(second.joined)
        assertEquals(first.clusterId, second.clusterId)
    }

    @Test
    fun `다른 사건이면 신규 클러스터`() {
        assigner.assign(entry("a1", "삼성전자 3나노 수주"))
        val second = assigner.assign(entry("b1", "카카오 데이터센터 화재 복구", source = "maeil"))
        assertTrue(second.created)
        assertEquals(2, store.clusters.size)
    }

    @Test
    fun `같은 sourceId 재할당은 기존 클러스터 반환`() {
        val first = assigner.assign(entry("a1", "삼성전자 3나노 수주"))
        val again = assigner.assign(entry("a1", "삼성전자 3나노 수주"))
        assertEquals(first.clusterId, again.clusterId)
        assertFalse(again.created)
        assertFalse(again.joined)
        assertEquals(1, store.articles.size)
    }

    @Test
    fun `같은 제목이어도 후보 종목이 다르면 다른 클러스터`() {
        assigner.assign(entry("a1", "실적 전망 상향", codes = listOf("005930")))
        val second = assigner.assign(entry("b1", "실적 전망 상향", codes = listOf("000660")))
        assertTrue(second.created)
        assertEquals(2, store.clusters.size)
    }

    @Test
    fun `매크로 기사와 종목 기사는 같은 제목이어도 분리`() {
        assigner.assign(entry("a1", "기준금리 동결", codes = listOf("005930")))
        val macro = assigner.assign(entry("b1", "기준금리 동결", codes = emptyList(), macroHint = "금리"))
        assertTrue(macro.created)
        assertEquals(2, store.clusters.size)
    }

    @Test
    fun `IRRELEVANT 클러스터는 후속 기사의 후보에서 제외`() {
        val first = assigner.assign(entry("a1", "삼성 라이온즈 우승"))
        store.clusters.getValue(first.clusterId).status = ClusterStatus.IRRELEVANT
        val second = assigner.assign(entry("b1", "삼성 라이온즈 우승"))
        assertTrue(second.created)
        assertEquals(2, store.clusters.size)
    }

    @Test
    fun `같은 sourceId 재처리는 누락된 후보 종목을 복구`() {
        val first = assigner.assign(entry("a1", "반도체 업황"))
        assigner.assign(entry("a1", "반도체 업황", codes = listOf("000660")))
        assertEquals(setOf("005930", "000660"), store.stockLinks(first.clusterId).map { it.code }.toSet())
    }

    @Test
    fun `sourceId 삽입 경쟁에서 진 클러스터는 빈 행을 남기지 않는다`() {
        val backing = InMemoryClusterStore()
        val winnerId = "winner".padEnd(26, '0')
        val racingStore = object : ClusterStore by backing {
            private var findCount = 0

            override fun findArticleCluster(sourceId: String): ClusterRecord? {
                findCount++
                return if (findCount <= 2) null else backing.findArticleCluster(sourceId)
            }

            override fun attachArticle(article: ArticleRecord, clusterId: String, embedding: FloatArray?): Boolean {
                backing.createCluster(winnerId, article.title, article.publishedAt)
                backing.attachArticle(article, winnerId, embedding)
                return false
            }
        }
        val racingAssigner = ClusterAssigner(
            racingStore,
            FakeEmbeddingClient(4096),
            NoopClusterLock(),
            clusterProps,
            { "loser".padEnd(26, '0') },
            clock,
        )

        val result = racingAssigner.assign(entry("race", "동일 기사"))

        assertEquals(winnerId, result.clusterId)
        assertEquals(setOf(winnerId), backing.clusters.keys)
    }
}
