package com.alphatalk.contracts

import kotlin.test.Test
import kotlin.test.assertEquals

class KeysQueuesTest {
    @Test
    fun `관심목록 미러 키 생성 - Redis 계약 §3`() {
        assertEquals("watchlist:123", Keys.watchlist(123))
        assertEquals("watchlist:rev:123", Keys.watchlistRev(123))
    }

    @Test
    fun `뉴스 파이프라인 키 생성`() {
        assertEquals("seen:ingest:hankyung:a1b2", Keys.seenIngest("hankyung:a1b2"))
        assertEquals("lock:cluster:005930", Keys.clusterLock("005930"))
        assertEquals("rate:article-fetch:news.example.com", Keys.articleFetchRate("news.example.com"))
    }

    @Test
    fun `분봉 수집 키 생성 - Redis 계약 §3`() {
        assertEquals("lock:minute-refresh:005930", Keys.minuteRefreshLock("005930"))
        assertEquals("minute:through:005930:20260806", Keys.minuteRefreshWatermark("005930", "20260806"))
        assertEquals("minute:market-div:005930:20260806", Keys.minuteMarketDiv("005930", "20260806"))
        assertEquals("rate:kis-rest:real-1", Keys.kisRestRate("real-1"))
    }

    @Test
    fun `메인서버 전용 키 생성 - Redis 계약 §3 주석`() {
        assertEquals("cursor:123:005930", Keys.cursor(123, "005930"))
        assertEquals("rl:post:123:29552131", Keys.rateLimitWindow("post", "123", 29552131))
        assertEquals("badge:123", Keys.badge(123))
    }

    @Test
    fun `큐 상수 - Redis 계약 §2`() {
        assertEquals("queue:ingest", Queues.INGEST)
        assertEquals("queue:ingest:dlq", Queues.INGEST_DLQ)
        assertEquals("g:llm", Queues.INGEST_GROUP_LLM)
    }
}
