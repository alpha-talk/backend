package com.alphatalk.worker.ingest

import com.alphatalk.worker.ingest.config.IngestProperties
import com.alphatalk.worker.ingest.scheduler.IngestPoller
import com.alphatalk.worker.ingest.source.NewsSource
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull

@SpringBootTest(properties = ["alphatalk.ingest.poll-enabled=false"])
class IngestWorkerApplicationTest {
    @Autowired
    private lateinit var properties: IngestProperties

    @Autowired
    private lateinit var poller: IngestPoller

    @Autowired
    private lateinit var newsSources: List<NewsSource>

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
    fun `기본 프로필에 경제 사회 RSS 피드가 등록된다`() {
        assertEquals(24, properties.feeds.size)
        assertEquals(
            setOf("yna", "hankyung", "mk", "donga", "khan", "hani", "chosun", "newsis", "sedaily"),
            properties.feeds.map { it.source }.toSet(),
        )
        assertEquals(properties.feeds.size, properties.feeds.map { it.id }.distinct().size)
        assertFalse(properties.feeds.any { "politics" in it.id })
    }
}
