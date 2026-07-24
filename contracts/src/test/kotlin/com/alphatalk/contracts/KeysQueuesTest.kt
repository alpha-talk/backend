package com.alphatalk.contracts

import kotlin.test.Test
import kotlin.test.assertEquals

class KeysQueuesTest {
    @Test
    fun `뉴스 파이프라인 키 생성`() {
        assertEquals("seen:ingest:hankyung:a1b2", Keys.seenIngest("hankyung:a1b2"))
        assertEquals("lock:cluster:005930", Keys.clusterLock("005930"))
        assertEquals("rate:article-fetch:news.example.com", Keys.articleFetchRate("news.example.com"))
    }

    @Test
    fun `큐 상수 - Redis 계약 §2`() {
        assertEquals("queue:ingest", Queues.INGEST)
        assertEquals("queue:ingest:dlq", Queues.INGEST_DLQ)
        assertEquals("g:llm", Queues.INGEST_GROUP_LLM)
    }
}
