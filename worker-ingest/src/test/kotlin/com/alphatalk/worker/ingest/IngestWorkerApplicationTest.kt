package com.alphatalk.worker.ingest

import com.alphatalk.worker.ingest.config.IngestProperties
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertFalse

@SpringBootTest(properties = ["alphatalk.ingest.poll-enabled=false"])
class IngestWorkerApplicationTest {
    @Autowired
    private lateinit var properties: IngestProperties

    @Test
    fun `컨텍스트 로드`() {
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
