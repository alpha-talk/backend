package com.alphatalk.worker.llm.cluster

import com.alphatalk.contracts.queue.IngestQueueEntry
import com.alphatalk.contracts.queue.IngestType
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ClusterAssignerTest {
    private val store = InMemoryClusterStore()
    private val clock = Clock.fixed(Instant.parse("2026-07-16T09:00:00Z"), ZoneOffset.UTC)
    private var idCounter = 0
    private val assigner = ClusterAssigner(
        store = store,
        embeddings = FakeEmbeddingClient(4096),
        lock = NoopClusterLock(),
        window = Duration.ofHours(72),
        similarityThreshold = 0.85,
        clusterIds = { "cluster-${++idCounter}".padEnd(26, '0') },
        clock = clock,
    )

    private fun entry(sourceId: String, title: String, source: String = "hankyung") = IngestQueueEntry(
        source = source,
        sourceId = sourceId,
        type = IngestType.NEWS,
        codes = listOf("005930"),
        title = title,
        url = "https://example.com/$sourceId",
        fetchedAt = clock.millis(),
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
}
